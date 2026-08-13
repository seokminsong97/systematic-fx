"""Fail-closed verifier for the externally provisioned sealed-holdout boundary."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row


class HoldoutIsolationError(RuntimeError):
    """The database session cannot prove a non-privileged research boundary."""


@dataclass(frozen=True, slots=True)
class HoldoutIsolationReport:
    session_user: str
    current_user: str
    role_name: str
    schema_usage: bool
    table_select: bool
    direct_read_denied: bool
    executor_membership: bool
    reachable_role_count: int
    forbidden_write_count: int = 0
    status: str = "ACCESS_DENIED_VERIFIED"


def verify_research_holdout_isolation(
    database_url: str,
    *,
    expected_session_user: str,
    expected_role: str = "systematic_fx_research_daemon",
) -> HoldoutIsolationReport:
    """Prove denial from the actual daemon login, never via superuser ``SET ROLE``.

    This function accepts only the research database URL.  A holdout executor URL is
    deliberately not part of the daemon API.
    """

    if not database_url.strip() or not expected_session_user.strip() or not expected_role.strip():
        raise HoldoutIsolationError("database URL, expected login, and role must be non-empty")
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.read_only = True
        with connection.transaction():
            try:
                identity = connection.execute(
                    """
                SELECT session_user::text AS session_user,
                       current_user::text AS current_user,
                       activity.usename::text AS backend_user,
                       role.rolname, role.rolsuper, role.rolbypassrls,
                       role.rolcreatedb, role.rolcreaterole, role.rolreplication,
                       pg_has_role(session_user, %s, 'MEMBER') AS research_member,
                       pg_has_role(session_user, 'systematic_fx_holdout_executor', 'MEMBER')
                           AS executor_member,
                       (has_schema_privilege(
                            session_user, sealed_namespace.oid, 'USAGE')
                        OR has_schema_privilege(
                            session_user, sealed_namespace.oid, 'CREATE'))
                           AS schema_usage,
                       has_schema_privilege(
                           session_user, 'systematic_fx', 'CREATE')
                           AS main_schema_create,
                       has_table_privilege(
                           session_user,
                           sealed_relation.oid,
                           'SELECT'
                       ) AS table_select
                FROM pg_catalog.pg_roles AS role
                CROSS JOIN pg_catalog.pg_class AS sealed_relation
                JOIN pg_catalog.pg_namespace AS sealed_namespace
                  ON sealed_namespace.oid = sealed_relation.relnamespace
                LEFT JOIN pg_catalog.pg_stat_activity AS activity
                  ON activity.pid = pg_backend_pid()
                WHERE role.rolname = session_user
                  AND sealed_namespace.nspname = 'systematic_fx_sealed'
                  AND sealed_relation.relname = 'holdout_artifacts'
                """,
                    (expected_role,),
                ).fetchone()
            except (psycopg.errors.InvalidSchemaName, psycopg.errors.UndefinedTable) as error:
                raise HoldoutIsolationError(
                    "sealed holdout schema/table is not provisioned"
                ) from error
            if identity is None:
                raise HoldoutIsolationError("database session role is absent from pg_roles")
            if identity["session_user"] != expected_session_user:
                raise HoldoutIsolationError("research connection used an unexpected session_user")
            if identity["current_user"] != expected_session_user:
                raise HoldoutIsolationError("SET ROLE sessions do not prove credential isolation")
            if identity["backend_user"] != expected_session_user:
                raise HoldoutIsolationError(
                    "backend authentication identity differs from session_user"
                )
            if identity["rolname"] != expected_session_user:
                raise HoldoutIsolationError("session identity query drifted")
            privileged = any(
                bool(identity[field])
                for field in (
                    "rolsuper",
                    "rolbypassrls",
                    "rolcreatedb",
                    "rolcreaterole",
                    "rolreplication",
                )
            )
            if privileged or not bool(identity["research_member"]):
                raise HoldoutIsolationError(
                    "research login is privileged or lacks the restricted research group"
                )
            if bool(identity["executor_member"]):
                raise HoldoutIsolationError("research login inherits the holdout executor role")
            reachable = connection.execute(
                """
                SELECT target.rolname,
                       target.rolsuper, target.rolbypassrls,
                       target.rolcreatedb, target.rolcreaterole,
                       target.rolreplication,
                       (has_schema_privilege(
                            target.rolname, sealed_namespace.oid, 'USAGE')
                        OR has_schema_privilege(
                            target.rolname, sealed_namespace.oid, 'CREATE'))
                           AS schema_usage,
                       (has_table_privilege(
                            target.rolname,
                            sealed_relation.oid, 'SELECT')
                        OR has_table_privilege(
                            target.rolname,
                            sealed_relation.oid, 'INSERT')
                        OR has_table_privilege(
                            target.rolname,
                            sealed_relation.oid, 'UPDATE')
                        OR has_table_privilege(
                            target.rolname,
                            sealed_relation.oid, 'DELETE')
                        OR has_table_privilege(
                            target.rolname,
                            sealed_relation.oid, 'TRUNCATE')
                        OR has_table_privilege(
                            target.rolname,
                            sealed_relation.oid, 'REFERENCES')
                        OR has_table_privilege(
                            target.rolname,
                            sealed_relation.oid, 'TRIGGER'))
                           AS table_privilege,
                       target.rolname IN (
                           'systematic_fx_holdout_owner',
                           'systematic_fx_holdout_executor'
                       ) AS holdout_role
                  FROM pg_catalog.pg_roles AS target
                  CROSS JOIN pg_catalog.pg_class AS sealed_relation
                  JOIN pg_catalog.pg_namespace AS sealed_namespace
                    ON sealed_namespace.oid = sealed_relation.relnamespace
                 WHERE target.rolname <> session_user
                   AND sealed_namespace.nspname = 'systematic_fx_sealed'
                   AND sealed_relation.relname = 'holdout_artifacts'
                   AND pg_has_role(session_user, target.rolname, 'MEMBER')
                 ORDER BY target.rolname
                """
            ).fetchall()
            unsafe_reachable = any(
                bool(row[field])
                for row in reachable
                for field in (
                    "rolsuper",
                    "rolbypassrls",
                    "rolcreatedb",
                    "rolcreaterole",
                    "rolreplication",
                    "schema_usage",
                    "table_privilege",
                    "holdout_role",
                )
            )
            unexpected_reachable = any(row["rolname"] != expected_role for row in reachable)
            if unsafe_reachable or unexpected_reachable:
                raise HoldoutIsolationError(
                    "research login can SET ROLE into a sealed or privileged identity"
                )
            # NOINHERIT does not remove SET ROLE authority.  Every capability
            # check below therefore covers both the authenticated login and the
            # one explicitly allowlisted reachable research group.
            principals = [expected_session_user, expected_role]
            if bool(identity["schema_usage"]) or bool(identity["table_select"]):
                raise HoldoutIsolationError("research login has sealed-holdout privileges")
            schema_creators = connection.execute(
                """
                SELECT principal.role_name
                  FROM unnest(%s::text[]) AS principal(role_name)
                  JOIN pg_catalog.pg_namespace AS namespace
                    ON namespace.nspname = 'systematic_fx'
                 WHERE has_schema_privilege(
                     principal.role_name, namespace.oid, 'CREATE')
                 ORDER BY principal.role_name
                """,
                (principals,),
            ).fetchall()
            if bool(identity["main_schema_create"]) or schema_creators:
                raise HoldoutIsolationError(
                    "research login can create objects in the governed application schema"
                )
            forbidden_tables = (
                "datasets",
                "source_files",
                "instruments",
                "campaigns",
                "campaign_splits",
                "campaign_days",
                "pattern_ledger",
                "experiment_trials",
                "strategies",
                "backtest_runs",
                "experiments",
                "m0b_epochs",
                "artifacts",
                "research_run_specs",
                "research_run_attempts",
                "m0b_candidates",
                "m0b_checkpoints",
                "m0b_artifact_links",
            )
            forbidden_write_rows = connection.execute(
                """
                SELECT principal.role_name, governed.table_name
                  FROM unnest(%s::text[]) AS principal(role_name)
                  CROSS JOIN unnest(%s::text[]) AS governed(table_name)
                  JOIN pg_catalog.pg_namespace AS namespace
                    ON namespace.nspname = 'systematic_fx'
                  JOIN pg_catalog.pg_class AS relation
                    ON relation.relnamespace = namespace.oid
                   AND relation.relname = governed.table_name
                 WHERE has_table_privilege(
                    principal.role_name,
                    relation.oid,
                    'INSERT')
                    OR has_table_privilege(
                    principal.role_name,
                    relation.oid,
                    'UPDATE')
                    OR has_table_privilege(
                    principal.role_name,
                    relation.oid,
                    'DELETE')
                    OR has_table_privilege(
                    principal.role_name,
                    relation.oid,
                    'TRUNCATE')
                 ORDER BY principal.role_name, governed.table_name
                """,
                (principals, list(forbidden_tables)),
            ).fetchall()
            if forbidden_write_rows:
                raise HoldoutIsolationError(
                    "research login has direct promotion, holdout, or input-catalog DML"
                )
            executable_definers = connection.execute(
                """
                SELECT principal.role_name, namespace.nspname, routine.proname,
                       owner.rolname AS owner_name
                  FROM unnest(%s::text[]) AS principal(role_name)
                  CROSS JOIN pg_catalog.pg_proc AS routine
                  JOIN pg_catalog.pg_namespace AS namespace
                    ON namespace.oid = routine.pronamespace
                  JOIN pg_catalog.pg_roles AS owner ON owner.oid = routine.proowner
                 WHERE routine.prosecdef
                   AND namespace.nspname NOT IN ('pg_catalog', 'information_schema')
                   AND has_function_privilege(
                       principal.role_name, routine.oid, 'EXECUTE')
                 ORDER BY principal.role_name, namespace.nspname, routine.proname
                """,
                (principals,),
            ).fetchall()
            if executable_definers:
                raise HoldoutIsolationError(
                    "research login can execute a SECURITY DEFINER database capability"
                )
            readable_derived_relations = connection.execute(
                """
                WITH RECURSIVE derived(oid) AS (
                    SELECT sealed_relation.oid
                      FROM pg_catalog.pg_class AS sealed_relation
                      JOIN pg_catalog.pg_namespace AS sealed_namespace
                        ON sealed_namespace.oid = sealed_relation.relnamespace
                     WHERE sealed_namespace.nspname = 'systematic_fx_sealed'
                       AND sealed_relation.relname = 'holdout_artifacts'
                    UNION
                    SELECT rewrite.ev_class
                      FROM derived AS parent
                      JOIN pg_catalog.pg_depend AS dependency
                        ON dependency.refobjid = parent.oid
                       AND dependency.classid = 'pg_rewrite'::regclass
                      JOIN pg_catalog.pg_rewrite AS rewrite
                        ON rewrite.oid = dependency.objid
                )
                SELECT principal.role_name, namespace.nspname, relation.relname
                  FROM unnest(%s::text[]) AS principal(role_name)
                  CROSS JOIN derived
                  JOIN pg_catalog.pg_class AS relation USING (oid)
                  JOIN pg_catalog.pg_namespace AS namespace
                    ON namespace.oid = relation.relnamespace
                 WHERE namespace.nspname <> 'systematic_fx_sealed'
                   AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
                   AND has_table_privilege(
                       principal.role_name, relation.oid, 'SELECT')
                 ORDER BY principal.role_name, namespace.nspname, relation.relname
                """,
                (principals,),
            ).fetchall()
            if readable_derived_relations:
                raise HoldoutIsolationError(
                    "research login can read a view derived from sealed holdout data"
                )
            external_owner_relations = connection.execute(
                """
                SELECT namespace.nspname, relation.relname
                  FROM pg_catalog.pg_class AS relation
                  JOIN pg_catalog.pg_namespace AS namespace
                    ON namespace.oid = relation.relnamespace
                  JOIN pg_catalog.pg_roles AS owner ON owner.oid = relation.relowner
                 WHERE owner.rolname = 'systematic_fx_holdout_owner'
                   AND namespace.nspname <> 'systematic_fx_sealed'
                   AND namespace.nspname NOT IN ('pg_catalog', 'information_schema')
                   AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
                 ORDER BY namespace.nspname, relation.relname
                """
            ).fetchall()
            if external_owner_relations:
                raise HoldoutIsolationError(
                    "holdout owner controls relations outside the sealed schema"
                )
            direct_read_denied = False
            try:
                with connection.transaction():
                    connection.execute(
                        "SELECT holdout_artifact_id "
                        "FROM systematic_fx_sealed.holdout_artifacts LIMIT 1"
                    ).fetchone()
            except psycopg.errors.InsufficientPrivilege:
                direct_read_denied = True
            if not direct_read_denied:
                raise HoldoutIsolationError("research login unexpectedly read sealed holdout")
    return HoldoutIsolationReport(
        session_user=expected_session_user,
        current_user=expected_session_user,
        role_name=expected_role,
        schema_usage=False,
        table_select=False,
        direct_read_denied=True,
        executor_membership=False,
        reachable_role_count=len(reachable),
        forbidden_write_count=0,
    )
