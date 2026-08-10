from pathlib import Path

from systematic_fx.db.bar_state_registry import (
    BAR_STATE_CAMPAIGN_DEFINITION_SHA256,
    BAR_STATE_CANDIDATE_CATALOG_SHA256,
)
from systematic_fx.db.migrations import discover_migrations
from systematic_fx.research.bar_state_config import (
    BAR_STATE_CONFIG_FILE_SHA256,
    BAR_STATE_CONFIG_SEMANTIC_SHA256,
    load_bar_state_config,
)
from systematic_fx.validation.bar_state_splits import BAR_STATE_FROZEN_SPLIT_SHA256

ROOT = Path(__file__).resolve().parents[2]


def test_migration_0024_pins_every_approved_preregistration_identity() -> None:
    migrations = {item.version: item for item in discover_migrations(ROOT / "migrations")}
    migration = migrations[24]
    assert migration.name == "bar_state_conditional_governance"
    sql = migration.path.read_text(encoding="utf-8")

    for identity in (
        BAR_STATE_CONFIG_FILE_SHA256,
        BAR_STATE_CONFIG_SEMANTIC_SHA256,
        BAR_STATE_CANDIDATE_CATALOG_SHA256,
        BAR_STATE_CAMPAIGN_DEFINITION_SHA256,
        BAR_STATE_FROZEN_SPLIT_SHA256,
    ):
        assert f"'{identity}'" in sql
    config = load_bar_state_config(ROOT)
    for candidate in config.candidates:
        assert f"WHEN '{candidate.candidate_key}' THEN" in sql
        assert f"'{candidate.definition_sha256}'" in sql


def test_migration_0024_is_discovery_only_and_requires_all_twelve_candidates() -> None:
    migration = {item.version: item for item in discover_migrations(ROOT / "migrations")}[24]
    sql = migration.path.read_text(encoding="utf-8")

    assert "CREATE TABLE systematic_fx.bar_state_artifact_links" in sql
    assert "systematic_fx.bar_state_catalog_preregistered" in sql
    assert "SELECT count(*)" in sql
    assert ") = 12" in sql
    assert "'DISCOVERY_ONLY'" in sql
    assert "'discovery', 'discovery_inner_1'" in sql
    for role in (
        "FEATURE",
        "LABEL",
        "MODEL",
        "OOS_TRADE",
        "GLOBAL_RESULT",
        "TERMINAL_RESULT",
    ):
        assert f"'{role}'" in sql
    assert "bar_state_artifact_links_publication_refresh" in sql
    assert "systematic_fx.request_publication_refresh()" in sql
    assert "VALUES ('public-research', 1, 0, statement_timestamp())" in sql


def test_migration_0024_fails_closed_on_lineage_consensus_and_terminal_pair() -> None:
    migration = {item.version: item for item in discover_migrations(ROOT / "migrations")}[24]
    sql = migration.path.read_text(encoding="utf-8")

    assert "UNRESOLVED_AT_BOUNDARY_CENSORED_PRIOR_FIRST_TOUCH_PRESERVED" in sql
    assert "systematic_fx.jsonb_has_exact_keys(" in sql
    assert "'{logical_identity,candidate_key}' IS DISTINCT FROM candidate_key" in sql
    assert "IS DISTINCT FROM expected_candidate_definition_sha256" in sql
    for expected_lineage in (
        "expected_code_snapshot_sha256",
        "expected_dependency_lock_sha256",
        "expected_runtime_environment_sha256",
        "expected_ordered_run_set_sha256",
    ):
        assert sql.count(expected_lineage) >= 3
    assert "FOR UPDATE;" in sql
    assert "bar-state candidates require one exact global result" in sql
    assert "attempt.result_summary = trial.result_summary" in sql
    assert "attempt.result_artifact_id = terminal_link.artifact_id" in sql
    assert "attempt.result_summary #>> '{trial_status}' = trial.status" in sql
    assert "'{logical_identity,decision_label}' =" in sql
    assert "'{logical_identity,trial_status}' =" in sql
    assert "role_counts IS DISTINCT FROM jsonb_build_object(" in sql
    assert "IS DISTINCT FROM ARRAY[0, 1, 2, 3]" in sql
    assert "systematic_fx.bar_state_economic_multiplier" in sql
    assert "candidate_selection_sha256_by_key" in sql
    assert "finalist_model_binding_by_key" in sql
    assert "finalist_model_binding_sha256_by_key" in sql
    assert "'{logical_identity,candidate_selection_sha256}'" in sql
    assert "'{logical_identity,finalist_model_binding_sha256}'" in sql
    assert "'[\"MAXIMUM_FINALIST_LIMIT\"]'::jsonb" in sql
    assert "jsonb_object_length" not in sql
