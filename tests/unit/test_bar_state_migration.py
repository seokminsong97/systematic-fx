from pathlib import Path

from systematic_fx.db.bar_state_registry import (
    BAR_STATE_CAMPAIGN_DEFINITION_SHA256,
    BAR_STATE_CANDIDATE_CATALOG_SHA256,
)
from systematic_fx.db.migrations import discover_migrations
from systematic_fx.research.bar_state_config import (
    BAR_STATE_CONFIG_FILE_SHA256,
    BAR_STATE_CONFIG_SEMANTIC_SHA256,
    BAR_STATE_V2_PROFILE,
    BAR_STATE_V2A_PROFILE,
    BAR_STATE_V2B_PROFILE,
    load_bar_state_config,
)
from systematic_fx.research.hypotheses import canonical_sha256
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


def test_migration_0025_only_rebinds_the_dataset_row_to_the_raw_manifest() -> None:
    migrations = {item.version: item for item in discover_migrations(ROOT / "migrations")}
    governance = migrations[24]
    migration = migrations[25]
    assert migration.name == "bar_state_raw_dataset_lineage_fix"

    old_sql = governance.path.read_text(encoding="utf-8")
    new_sql = migration.path.read_text(encoding="utf-8")
    old_start = "CREATE FUNCTION systematic_fx.bar_state_run_spec_matches_trial("
    new_start = "CREATE OR REPLACE FUNCTION systematic_fx.bar_state_run_spec_matches_trial("
    old_end = "CREATE FUNCTION systematic_fx.bar_state_catalog_preregistered("
    new_end = "INSERT INTO systematic_fx.schema_migrations"
    old_function = old_sql.split(old_start, maxsplit=1)[1].split(old_end, maxsplit=1)[0]
    new_function = new_sql.split(new_start, maxsplit=1)[1].split(new_end, maxsplit=1)[0]
    old_predicate = "dataset.manifest_sha256 = campaign.data_manifest_sha256"
    new_predicate = "dataset.manifest_sha256 = trial.parameters #>> '{raw_source_manifest_sha256}'"

    assert old_function.count(old_predicate) == 1
    assert new_predicate not in old_function
    assert "pg_get_functiondef" not in new_sql
    assert (
        new_function.strip()
        == old_function.replace(
            old_predicate,
            new_predicate,
        ).strip()
    )
    assert "VALUES (25, 'bar_state_raw_dataset_lineage_fix', :'migration_checksum')" in new_sql


def test_migration_0026_dispatches_exact_v2_and_v2a_profiles() -> None:
    migrations = {item.version: item for item in discover_migrations(ROOT / "migrations")}
    migration = migrations[26]
    assert migration.name == "bar_state_v2a_optimizer_cap_amendment"
    assert migration.checksum == (
        "232badda3e76fca79f93fcff059de6f3404fc797eb26a93c9483fd554cfe20bb"
    )
    sql = migration.path.read_text(encoding="utf-8")

    for profile in (BAR_STATE_V2_PROFILE, BAR_STATE_V2A_PROFILE):
        config = load_bar_state_config(ROOT, profile=profile)
        for identity in (
            profile.campaign_key,
            profile.campaign_name,
            profile.experiment_key,
            profile.artifact_type,
            profile.engine_version,
            profile.config_file_sha256,
            profile.config_semantic_sha256,
            profile.candidate_catalog_sha256,
            profile.campaign_definition_sha256,
        ):
            assert f"'{identity}'" in sql
        model_policy_sha256s = {
            canonical_sha256(candidate.as_dict()["model_policy"]) for candidate in config.candidates
        }
        assert len(model_policy_sha256s) == 1
        assert f"'{model_policy_sha256s.pop()}'" in sql
        assert str(profile.model_max_iter) in sql
        for candidate in config.candidates:
            assert f"'{candidate.candidate_key}'" in sql
            assert f"'{candidate.definition_sha256}'" in sql

    for function_name in (
        "bar_state_experiment_is_governed",
        "bar_state_run_spec_matches_trial",
        "protect_bar_state_campaign_identity",
        "protect_bar_state_experiment_identity",
        "enforce_bar_state_trial_lifecycle",
        "enforce_bar_state_attempt_lifecycle",
        "enforce_bar_state_artifact_link",
        "require_bar_state_terminal_pair",
    ):
        assert f"CREATE OR REPLACE FUNCTION systematic_fx.{function_name}" in sql
    assert "FOR UPDATE;" in sql
    assert "dataset.manifest_sha256 = trial.parameters #>> '{raw_source_manifest_sha256}'" in sql
    assert "attempt.started_at IS NOT NULL" in sql
    assert "NOT EXISTS (" in sql
    assert "'FEATURE', 'LABEL', 'MODEL', 'OOS_TRADE'" in sql
    assert "profile_version = 'V2A'" in sql
    assert "research_run_specs_freeze_bar_state_v2_predecessor" in sql
    assert "campaigns_require_bar_state_v2a_predecessor" in sql
    assert "bar_state_governance_profile(OLD.campaign_key)" in sql
    assert "bar_state_governance_profile(NEW.campaign_key)" in sql
    assert "profile.experiment_key = NEW.experiment_key" in sql
    assert "VALUES (26, 'bar_state_v2a_optimizer_cap_amendment', :'migration_checksum')" in sql
    assert "PLACEHOLDER" not in sql


def test_migration_0027_dispatches_v2b_and_freezes_its_exact_v2a_predecessor() -> None:
    migrations = {item.version: item for item in discover_migrations(ROOT / "migrations")}
    migration = migrations[27]
    assert migration.name == "bar_state_v2b_parquet_schema_amendment"
    assert migration.checksum == (
        "f0f69db031dc555b260da1fceef5f1fb4087f25717f1472ae4b006e77182cdb8"
    )
    sql = migration.path.read_text(encoding="utf-8")

    assert migrations[24].checksum == (
        "4aa845757f1a220c8d5595d4db6053f6374d99d067ab7e20c3e40ea22d610010"
    )
    assert migrations[25].checksum == (
        "e08aa486bf9a65b2875e92866ae5e939fc56dc5d871010dfdb4b9085550749dd"
    )
    assert migrations[26].checksum == (
        "232badda3e76fca79f93fcff059de6f3404fc797eb26a93c9483fd554cfe20bb"
    )
    for profile in (BAR_STATE_V2_PROFILE, BAR_STATE_V2A_PROFILE, BAR_STATE_V2B_PROFILE):
        config = load_bar_state_config(ROOT, profile=profile)
        for identity in (
            profile.campaign_key,
            profile.campaign_name,
            profile.experiment_key,
            profile.artifact_type,
            profile.engine_version,
            profile.config_file_sha256,
            profile.config_semantic_sha256,
            profile.candidate_catalog_sha256,
            profile.campaign_definition_sha256,
        ):
            assert f"'{identity}'" in sql
        assert str(profile.model_max_iter) in sql
        for candidate in config.candidates:
            assert f"'{candidate.candidate_key}'" in sql
            assert f"'{candidate.definition_sha256}'" in sql

    assert BAR_STATE_V2B_PROFILE.predecessor_code_commit is not None
    assert f"'{BAR_STATE_V2B_PROFILE.predecessor_code_commit}'" in sql
    assert f"'{BAR_STATE_V2B_PROFILE.predecessor_gate_policy}'" in sql
    assert "CREATE FUNCTION systematic_fx.bar_state_preregistration_is_exact" in sql
    assert "bar_state_v2a_predecessor_is_clean_v26" in sql
    assert "campaign.selected_start_date = DATE '2022-01-03'" in sql
    assert "campaign.selected_end_date = DATE '2026-07-31'" in sql
    assert "campaign.roll_cutoff_date IS NULL" in sql
    assert "5da1027fb2003c521b4be2eee0d2bf1238e4784467f43f7d9b9ac978223f5552" in sql
    assert "8378983f7db68b443d385b7cc646f0294391293ccd1873dbc3a2458ad1384c49" in sql
    assert "ae3ab3f4e0a77e4e0ddf83d0bca969514f94734f0009ec85deb4cf573a490769" in sql
    assert "experiment.pattern_id IS NULL" in sql
    assert "experiment.parent_experiment_id IS NULL" in sql
    assert "experiment.tick_size = 0.00005" in sql
    assert "experiment.tick_value = 6.25" in sql
    assert "attempt.attempt_number IN (1, 2)" in sql
    assert "attempt.started_at IS NOT NULL" in sql
    assert ") = 24" in sql
    assert "attempt.result_summary = jsonb_build_object" in sql
    assert "profile_version IN ('V2A', 'V2B')" in sql
    assert "campaigns_require_bar_state_v2b_predecessor" in sql
    assert "aa_artifacts_freeze_bar_state_predecessor" in sql
    assert "BEFORE INSERT OR UPDATE ON systematic_fx.experiments" in sql
    assert "experiments_freeze_bar_state_predecessor" in sql
    assert "enforce_bar_state_v2_predecessor_runspec_freeze" in sql
    assert "experiment_trials_freeze_bar_state_predecessor" in sql
    assert "research_run_attempts_freeze_bar_state_v2a_predecessor" in sql
    assert "bar_state_artifact_links_freeze_predecessor" in sql
    assert "bar_state_artifact_links_enforce_v2b_feature_schema" in sql
    assert "^([0-9a-f]{40}|[0-9a-f]{64})$" in sql
    assert "da7e500759276e85483f070451595eb083f3c15e76541bc2a2bd86c6483ebef3" in sql
    assert "VALUES (27, 'bar_state_v2b_parquet_schema_amendment', :'migration_checksum')" in sql
    assert "PLACEHOLDER" not in sql
