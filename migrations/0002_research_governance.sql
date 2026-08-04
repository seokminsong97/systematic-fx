BEGIN;

CREATE TABLE systematic_fx.discovery_exposures (
    discovery_exposure_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    exposure_key text NOT NULL UNIQUE,
    campaign_id bigint NOT NULL,
    exposure_type text NOT NULL,
    source_interval_start timestamptz NOT NULL,
    source_interval_end timestamptz NOT NULL,
    visible_to_ai boolean NOT NULL DEFAULT true,
    research_eligible boolean NOT NULL DEFAULT false,
    query_spec jsonb NOT NULL,
    result_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    result_artifact_id bigint,
    code_commit text NOT NULL,
    config_sha256 text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT discovery_exposures_campaign_fk
        FOREIGN KEY (campaign_id) REFERENCES systematic_fx.campaigns(campaign_id),
    CONSTRAINT discovery_exposures_result_artifact_fk
        FOREIGN KEY (result_artifact_id) REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT discovery_exposures_key_nonempty CHECK (btrim(exposure_key) <> ''),
    CONSTRAINT discovery_exposures_type_valid
        CHECK (exposure_type IN
               ('AI_SLICE', 'QUERY', 'SUMMARY', 'EVENT_WINDOW', 'PIPELINE_PILOT')),
    CONSTRAINT discovery_exposures_interval_order
        CHECK (source_interval_start <= source_interval_end),
    CONSTRAINT discovery_exposures_query_object CHECK (jsonb_typeof(query_spec) = 'object'),
    CONSTRAINT discovery_exposures_result_object CHECK (jsonb_typeof(result_summary) = 'object'),
    CONSTRAINT discovery_exposures_code_commit_nonempty CHECK (btrim(code_commit) <> ''),
    CONSTRAINT discovery_exposures_config_sha256_valid
        CHECK (config_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX discovery_exposures_campaign_interval_idx
    ON systematic_fx.discovery_exposures
       (campaign_id, source_interval_start, source_interval_end);

ALTER TABLE systematic_fx.experiment_trials
    ADD COLUMN parameters_sha256 text;

ALTER TABLE systematic_fx.experiment_trials
    ADD CONSTRAINT experiment_trials_parameters_sha256_valid
        CHECK (parameters_sha256 IS NULL OR parameters_sha256 ~ '^[0-9a-f]{64}$');

ALTER TABLE systematic_fx.experiments
    ADD CONSTRAINT experiments_frozen_registration_required
        CHECK (status <> 'FROZEN'
               OR (frozen_at IS NOT NULL AND registration_artifact_id IS NOT NULL));

ALTER TABLE systematic_fx.strategies
    ADD CONSTRAINT strategies_nondraft_artifact_required
        CHECK (status = 'DRAFT' OR definition_artifact_id IS NOT NULL);

ALTER TABLE systematic_fx.derived_partitions
    ADD CONSTRAINT derived_partitions_validated_lineage_required
        CHECK (status <> 'VALIDATED'
               OR (validated_at IS NOT NULL
                   AND manifest_artifact_id IS NOT NULL
                   AND build_job_id IS NOT NULL));

COMMENT ON TABLE systematic_fx.discovery_exposures IS
    'Every AI-visible Discovery slice, query, summary, event window, and non-research pilot.';
COMMENT ON COLUMN systematic_fx.experiment_trials.parameters_sha256 IS
    'Canonical SHA-256 of immutable trial parameters; null only for legacy rows.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (2, 'research_governance', :'migration_checksum');

COMMIT;
