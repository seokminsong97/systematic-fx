from __future__ import annotations

from contextlib import nullcontext

import pytest

from systematic_fx.db import holdout_isolation


class _Result:
    def __init__(self, row=None, rows=None, error: Exception | None = None):
        self.row = row
        self.rows = [] if rows is None else rows
        self.error = error

    def fetchone(self):
        if self.error is not None:
            raise self.error
        return self.row

    def fetchall(self):
        if self.error is not None:
            raise self.error
        return self.rows


class _Connection:
    def __init__(self, identity):
        self.identity = identity
        self.read_only = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        del args

    def transaction(self):
        return nullcontext()

    def execute(self, query, parameters=None):
        del parameters
        if (
            "SELECT principal.role_name" in query
            and "has_schema_privilege" in query
            and "FROM unnest" in query
            and "pg_catalog.pg_proc" not in query
            and "WITH RECURSIVE derived" not in query
        ):
            return _Result(rows=self.identity.pop("schema_creators", []))
        if "pg_catalog.pg_proc AS routine" in query:
            return _Result(rows=self.identity.pop("executable_definers", []))
        if "WITH RECURSIVE derived" in query:
            return _Result(rows=self.identity.pop("readable_derived_relations", []))
        if "FROM unnest" in query:
            return _Result(rows=self.identity.pop("forbidden_writes", []))
        if "owner.rolname = 'systematic_fx_holdout_owner'" in query:
            return _Result(rows=self.identity.pop("external_owner_relations", []))
        if "FROM pg_catalog.pg_roles" in query:
            if "target.rolname" in query:
                return _Result(rows=self.identity.pop("reachable", []))
            return _Result(self.identity)
        return _Result(error=holdout_isolation.psycopg.errors.InsufficientPrivilege())


def _identity(**updates):
    row = {
        "session_user": "research_login",
        "current_user": "research_login",
        "backend_user": "research_login",
        "rolname": "research_login",
        "rolsuper": False,
        "rolbypassrls": False,
        "rolcreatedb": False,
        "rolcreaterole": False,
        "rolreplication": False,
        "research_member": True,
        "executor_member": False,
        "schema_usage": False,
        "main_schema_create": False,
        "table_select": False,
    }
    row.update(updates)
    return row


def test_actual_unprivileged_login_and_denied_read_pass(monkeypatch) -> None:
    connection = _Connection(_identity())
    monkeypatch.setattr(holdout_isolation.psycopg, "connect", lambda *args, **kwargs: connection)

    report = holdout_isolation.verify_research_holdout_isolation(
        "postgresql:///test", expected_session_user="research_login"
    )

    assert report.status == "ACCESS_DENIED_VERIFIED"
    assert report.direct_read_denied
    assert report.reachable_role_count == 0
    assert connection.read_only


@pytest.mark.parametrize(
    "updates",
    (
        {"current_user": "systematic_fx_research_daemon"},
        {"rolsuper": True},
        {"rolbypassrls": True},
        {"research_member": False},
        {"executor_member": True},
        {"schema_usage": True},
        {"main_schema_create": True},
        {"table_select": True},
        {"backend_user": "original_superuser"},
    ),
)
def test_privilege_and_set_role_shortcuts_fail(monkeypatch, updates) -> None:
    connection = _Connection(_identity(**updates))
    monkeypatch.setattr(holdout_isolation.psycopg, "connect", lambda *args, **kwargs: connection)

    with pytest.raises(holdout_isolation.HoldoutIsolationError):
        holdout_isolation.verify_research_holdout_isolation(
            "postgresql:///test", expected_session_user="research_login"
        )


def test_noinherit_parent_with_holdout_select_fails(monkeypatch) -> None:
    identity = _identity(
        reachable=[
            {
                "rolname": "readable_parent",
                "rolsuper": False,
                "rolbypassrls": False,
                "rolcreatedb": False,
                "rolcreaterole": False,
                "rolreplication": False,
                "schema_usage": True,
                "table_privilege": True,
                "holdout_role": False,
            }
        ]
    )
    connection = _Connection(identity)
    monkeypatch.setattr(holdout_isolation.psycopg, "connect", lambda *args, **kwargs: connection)

    with pytest.raises(holdout_isolation.HoldoutIsolationError, match="SET ROLE"):
        holdout_isolation.verify_research_holdout_isolation(
            "postgresql:///test", expected_session_user="research_login"
        )


def test_direct_promotion_write_fails(monkeypatch) -> None:
    connection = _Connection(_identity(forbidden_writes=[{"table_name": "strategies"}]))
    monkeypatch.setattr(holdout_isolation.psycopg, "connect", lambda *args, **kwargs: connection)

    with pytest.raises(holdout_isolation.HoldoutIsolationError, match="promotion"):
        holdout_isolation.verify_research_holdout_isolation(
            "postgresql:///test", expected_session_user="research_login"
        )


def test_direct_governed_evidence_write_fails(monkeypatch) -> None:
    connection = _Connection(_identity(forbidden_writes=[{"table_name": "m0b_candidates"}]))
    monkeypatch.setattr(holdout_isolation.psycopg, "connect", lambda *args, **kwargs: connection)

    with pytest.raises(holdout_isolation.HoldoutIsolationError, match="promotion"):
        holdout_isolation.verify_research_holdout_isolation(
            "postgresql:///test", expected_session_user="research_login"
        )


def test_executable_security_definer_fails(monkeypatch) -> None:
    connection = _Connection(
        _identity(
            executable_definers=[
                {
                    "nspname": "public",
                    "proname": "leak_holdout",
                    "owner_name": "systematic_fx_holdout_owner",
                }
            ]
        )
    )
    monkeypatch.setattr(holdout_isolation.psycopg, "connect", lambda *args, **kwargs: connection)

    with pytest.raises(holdout_isolation.HoldoutIsolationError, match="SECURITY DEFINER"):
        holdout_isolation.verify_research_holdout_isolation(
            "postgresql:///test", expected_session_user="research_login"
        )


def test_readable_view_derived_from_holdout_fails(monkeypatch) -> None:
    connection = _Connection(
        _identity(readable_derived_relations=[{"nspname": "public", "relname": "leak_view"}])
    )
    monkeypatch.setattr(holdout_isolation.psycopg, "connect", lambda *args, **kwargs: connection)

    with pytest.raises(holdout_isolation.HoldoutIsolationError, match="view derived"):
        holdout_isolation.verify_research_holdout_isolation(
            "postgresql:///test", expected_session_user="research_login"
        )


def test_holdout_owner_external_relation_fails(monkeypatch) -> None:
    connection = _Connection(
        _identity(external_owner_relations=[{"nspname": "public", "relname": "leak_view"}])
    )
    monkeypatch.setattr(holdout_isolation.psycopg, "connect", lambda *args, **kwargs: connection)

    with pytest.raises(holdout_isolation.HoldoutIsolationError, match="outside"):
        holdout_isolation.verify_research_holdout_isolation(
            "postgresql:///test", expected_session_user="research_login"
        )
