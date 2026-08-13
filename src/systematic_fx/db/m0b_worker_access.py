"""Fail-closed verifier for the actual least-privilege M0b worker login."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row


class M0bWorkerAccessError(RuntimeError):
    """The authenticated session cannot prove the exact worker capability set."""


@dataclass(frozen=True, slots=True)
class M0bWorkerAccessReport:
    session_user: str
    current_user: str
    role_name: str
    capability_count: int
    direct_write_count: int
    sealed_access: bool
    status: str = "LEAST_PRIVILEGE_VERIFIED"


_WORKER_ROLE = "systematic_fx_m0b_worker"
_API_OWNER = "systematic_fx_m0b_worker_api_owner"
_CAPABILITIES = (
    "m0b_worker_checkpoint(bigint,bigint,text,integer,text,text,jsonb)",
    "m0b_worker_claim_next(text,text,text,integer)",
    "m0b_worker_fail(bigint,bigint,text,text,boolean)",
    "m0b_worker_terminalize(bigint,bigint,text,text,bigint,jsonb)",
)
_ALLOWED_READ_TABLES = (
    "artifacts",
    "campaigns",
    "m0b_admission_decisions",
    "m0b_artifact_links",
    "m0b_candidates",
    "m0b_checkpoints",
    "m0b_epochs",
    "research_run_attempts",
)


def verify_m0b_worker_access(
    database_url: str,
    *,
    expected_session_user: str,
) -> M0bWorkerAccessReport:
    """Verify the actual LOGIN, direct denial, and exact four-function allowlist."""

    if not database_url.strip() or not expected_session_user.strip():
        raise M0bWorkerAccessError("database URL and expected worker login are required")
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.read_only = True
        with connection.transaction():
            identity = connection.execute(
                """
                SELECT session_user::text AS session_user,
                       current_user::text AS current_user,
                       activity.usename::text AS backend_user,
                       role.rolsuper, role.rolcreatedb, role.rolcreaterole,
                       role.rolreplication, role.rolbypassrls,
                       pg_has_role(session_user, %s, 'MEMBER') AS worker_member,
                       pg_has_role(session_user, 'systematic_fx_research_daemon', 'MEMBER')
                           AS research_member,
                       pg_has_role(session_user, 'systematic_fx_holdout_executor', 'MEMBER')
                           AS executor_member,
                       pg_has_role(session_user, 'systematic_fx_holdout_owner', 'MEMBER')
                           AS holdout_owner_member,
                       pg_has_role(session_user, %s, 'MEMBER') AS api_owner_member,
                       has_schema_privilege(session_user, 'systematic_fx', 'CREATE')
                           AS main_schema_create,
                       has_database_privilege(
                           session_user, current_database(), 'CREATE')
                           AS database_create,
                       (has_schema_privilege(session_user, sealed.oid, 'USAGE')
                        OR has_schema_privilege(session_user, sealed.oid, 'CREATE'))
                           AS sealed_schema,
                       has_table_privilege(
                           session_user, holdout.oid,
                           'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
                           AS sealed_table
                  FROM pg_roles AS role
                  LEFT JOIN pg_stat_activity AS activity ON activity.pid = pg_backend_pid()
                  CROSS JOIN pg_namespace AS sealed
                  CROSS JOIN pg_class AS holdout
                 WHERE role.rolname = session_user
                   AND sealed.nspname = 'systematic_fx_sealed'
                   AND holdout.relnamespace = sealed.oid
                   AND holdout.relname = 'holdout_artifacts'
                """,
                (_WORKER_ROLE, _API_OWNER),
            ).fetchone()
            if identity is None:
                raise M0bWorkerAccessError("worker identity or sealed boundary is absent")
            if (
                identity["session_user"] != expected_session_user
                or identity["current_user"] != expected_session_user
                or identity["backend_user"] != expected_session_user
            ):
                raise M0bWorkerAccessError("connection did not authenticate as the expected LOGIN")
            if any(
                bool(identity[key])
                for key in (
                    "rolsuper",
                    "rolcreatedb",
                    "rolcreaterole",
                    "rolreplication",
                    "rolbypassrls",
                    "research_member",
                    "executor_member",
                    "holdout_owner_member",
                    "api_owner_member",
                    "main_schema_create",
                    "database_create",
                    "sealed_schema",
                    "sealed_table",
                )
            ) or not bool(identity["worker_member"]):
                raise M0bWorkerAccessError("worker LOGIN has privilege outside its capability role")

            capability_roles = connection.execute(
                """
                SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
                       rolinherit, rolreplication, rolbypassrls
                  FROM pg_roles
                 WHERE rolname IN (%s, %s)
                 ORDER BY rolname
                """,
                (_API_OWNER, _WORKER_ROLE),
            ).fetchall()
            if [row["rolname"] for row in capability_roles] != [
                _WORKER_ROLE,
                _API_OWNER,
            ] or any(
                bool(row[attribute])
                for row in capability_roles
                for attribute in (
                    "rolcanlogin",
                    "rolsuper",
                    "rolcreatedb",
                    "rolcreaterole",
                    "rolinherit",
                    "rolreplication",
                    "rolbypassrls",
                )
            ):
                raise M0bWorkerAccessError("worker capability role attributes drifted")

            capability_memberships = connection.execute(
                """
                SELECT source.rolname AS source_role, target.rolname AS target_role
                  FROM pg_roles AS source
                  CROSS JOIN pg_roles AS target
                 WHERE source.rolname IN (%s, %s)
                   AND source.oid <> target.oid
                   AND pg_has_role(source.oid, target.oid, 'MEMBER')
                 ORDER BY source.rolname, target.rolname
                """,
                (_API_OWNER, _WORKER_ROLE),
            ).fetchall()
            if capability_memberships:
                raise M0bWorkerAccessError("worker capability role membership drifted")

            reachable = connection.execute(
                """
                SELECT role.rolname
                  FROM pg_roles AS role
                 WHERE role.rolname <> session_user
                   AND pg_has_role(session_user, role.rolname, 'MEMBER')
                 ORDER BY role.rolname
                """
            ).fetchall()
            if [row["rolname"] for row in reachable] != [_WORKER_ROLE]:
                raise M0bWorkerAccessError("worker LOGIN can SET ROLE outside its allowlist")

            writable_schemas = connection.execute(
                """
                SELECT namespace.nspname
                  FROM pg_namespace AS namespace
                 WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
                   AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
                   AND has_schema_privilege(session_user, namespace.oid, 'CREATE')
                 ORDER BY namespace.nspname
                """
            ).fetchall()
            if writable_schemas:
                raise M0bWorkerAccessError("worker LOGIN can create durable schema objects")

            direct_writes = connection.execute(
                """
                SELECT namespace.nspname, relation.relname
                  FROM pg_class AS relation
                  JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                 WHERE relation.relkind IN ('r', 'p', 'v', 'm', 'f')
                   AND namespace.nspname NOT IN ('pg_catalog', 'information_schema')
                   AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
                   AND has_table_privilege(
                       session_user, relation.oid,
                       'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
                 ORDER BY namespace.nspname, relation.relname
                """
            ).fetchall()
            writable_sequences = connection.execute(
                """
                SELECT namespace.nspname, relation.relname
                  FROM pg_class AS relation
                  JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                 WHERE relation.relkind = 'S'
                   AND namespace.nspname NOT IN ('pg_catalog', 'information_schema')
                   AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
                   AND has_sequence_privilege(
                       session_user, relation.oid, 'SELECT,USAGE,UPDATE')
                 ORDER BY namespace.nspname, relation.relname
                """
            ).fetchall()
            if direct_writes or writable_sequences:
                raise M0bWorkerAccessError("worker LOGIN has forbidden direct object mutation")

            readable = connection.execute(
                """
                SELECT namespace.nspname, relation.relname
                  FROM pg_class AS relation
                  JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                 WHERE relation.relkind IN ('r', 'p', 'v', 'm', 'f')
                   AND namespace.nspname NOT IN ('pg_catalog', 'information_schema')
                   AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
                   AND has_table_privilege(session_user, relation.oid, 'SELECT')
                 ORDER BY namespace.nspname, relation.relname
                """
            ).fetchall()
            expected_reads = tuple(
                ("systematic_fx", table_name) for table_name in _ALLOWED_READ_TABLES
            )
            observed_reads = tuple((row["nspname"], row["relname"]) for row in readable)
            if observed_reads != expected_reads:
                raise M0bWorkerAccessError("worker direct read allowlist drifted")

            capabilities = connection.execute(
                """
                SELECT namespace.nspname,
                       replace(routine.oid::regprocedure::text,
                               namespace.nspname || '.', '') AS signature,
                       owner.rolname AS owner_name,
                       routine.prosecdef,
                       routine.proconfig
                  FROM pg_proc AS routine
                  JOIN pg_namespace AS namespace ON namespace.oid = routine.pronamespace
                  JOIN pg_roles AS owner ON owner.oid = routine.proowner
                 WHERE routine.prosecdef
                   AND namespace.nspname NOT IN ('pg_catalog', 'information_schema')
                   AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
                   AND has_function_privilege(session_user, routine.oid, 'EXECUTE')
                 ORDER BY signature
                """
            ).fetchall()
            signatures = tuple(row["signature"].replace(" ", "") for row in capabilities)
            if signatures != _CAPABILITIES or any(
                row["nspname"] != "systematic_fx"
                or row["owner_name"] != _API_OWNER
                or not bool(row["prosecdef"])
                or row["proconfig"] != ["search_path=pg_catalog, pg_temp"]
                for row in capabilities
            ):
                raise M0bWorkerAccessError("worker SECURITY DEFINER allowlist drifted")

            return M0bWorkerAccessReport(
                session_user=str(identity["session_user"]),
                current_user=str(identity["current_user"]),
                role_name=_WORKER_ROLE,
                capability_count=len(capabilities),
                direct_write_count=len(direct_writes),
                sealed_access=bool(identity["sealed_schema"] or identity["sealed_table"]),
            )
