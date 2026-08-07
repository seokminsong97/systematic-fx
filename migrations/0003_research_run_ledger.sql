BEGIN;

CREATE TABLE systematic_fx.research_run_specs (
    research_run_spec_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_fingerprint text NOT NULL UNIQUE,
    canonicalization_schema text NOT NULL,
    canonicalization_version integer NOT NULL,
    campaign_id bigint NOT NULL,
    experiment_id bigint,
    parent_run_spec_id bigint,
    run_kind text NOT NULL,
    engine_version text NOT NULL,
    canonical_spec jsonb NOT NULL,
    source_manifest_hashes jsonb NOT NULL,
    eligible_calendar_version text NOT NULL,
    eligible_calendar_sha256 text NOT NULL,
    split_version text NOT NULL,
    split_sha256 text NOT NULL,
    feature_version text NOT NULL,
    feature_sha256 text NOT NULL,
    outcome_version text NOT NULL,
    outcome_sha256 text NOT NULL,
    cost_version text NOT NULL,
    cost_sha256 text NOT NULL,
    execution_version text NOT NULL,
    execution_sha256 text NOT NULL,
    code_commit text NOT NULL,
    dependency_lock_sha256 text NOT NULL,
    deterministic_seed numeric(20, 0) NOT NULL,
    direction text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT research_run_specs_campaign_fk
        FOREIGN KEY (campaign_id) REFERENCES systematic_fx.campaigns(campaign_id),
    CONSTRAINT research_run_specs_campaign_identity
        UNIQUE (campaign_id, research_run_spec_id),
    CONSTRAINT research_run_specs_experiment_fk
        FOREIGN KEY (campaign_id, experiment_id)
        REFERENCES systematic_fx.experiments(campaign_id, experiment_id),
    CONSTRAINT research_run_specs_parent_fk
        FOREIGN KEY (campaign_id, parent_run_spec_id)
        REFERENCES systematic_fx.research_run_specs(campaign_id, research_run_spec_id),
    CONSTRAINT research_run_specs_fingerprint_valid
        CHECK (run_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT research_run_specs_schema_nonempty
        CHECK (btrim(canonicalization_schema) <> '' AND canonicalization_version > 0),
    CONSTRAINT research_run_specs_kind_valid
        CHECK (run_kind IN
               ('FEATURE_BUILD', 'OUTCOME_BUILD', 'AI_SLICE', 'QUERY', 'SCREEN',
                'BARRIER_SURFACE', 'MODEL_FIT', 'BACKTEST', 'WALK_FORWARD',
                'HOLDOUT', 'STRESS', 'VALIDATION')),
    CONSTRAINT research_run_specs_engine_version_nonempty
        CHECK (btrim(engine_version) <> ''),
    CONSTRAINT research_run_specs_spec_object
        CHECK (jsonb_typeof(canonical_spec) = 'object'),
    CONSTRAINT research_run_specs_source_hashes_object
        CHECK (jsonb_typeof(source_manifest_hashes) = 'object'
               AND source_manifest_hashes <> '{}'::jsonb),
    CONSTRAINT research_run_specs_versions_nonempty
        CHECK (btrim(eligible_calendar_version) <> ''
               AND btrim(split_version) <> ''
               AND btrim(feature_version) <> ''
               AND btrim(outcome_version) <> ''
               AND btrim(cost_version) <> ''
               AND btrim(execution_version) <> ''),
    CONSTRAINT research_run_specs_calendar_sha256_valid
        CHECK (eligible_calendar_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT research_run_specs_split_sha256_valid
        CHECK (split_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT research_run_specs_feature_sha256_valid
        CHECK (feature_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT research_run_specs_outcome_sha256_valid
        CHECK (outcome_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT research_run_specs_cost_sha256_valid
        CHECK (cost_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT research_run_specs_execution_sha256_valid
        CHECK (execution_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT research_run_specs_code_commit_nonempty
        CHECK (btrim(code_commit) <> ''),
    CONSTRAINT research_run_specs_dependency_lock_sha256_valid
        CHECK (dependency_lock_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT research_run_specs_seed_uint64
        CHECK (deterministic_seed >= 0
               AND deterministic_seed <= 18446744073709551615),
    CONSTRAINT research_run_specs_direction_valid
        CHECK (direction IN ('LONG', 'SHORT', 'BOTH'))
);

CREATE INDEX research_run_specs_campaign_kind_idx
    ON systematic_fx.research_run_specs (campaign_id, run_kind, created_at);
CREATE INDEX research_run_specs_experiment_idx
    ON systematic_fx.research_run_specs (experiment_id, run_kind)
    WHERE experiment_id IS NOT NULL;

CREATE TABLE systematic_fx.research_run_attempts (
    research_run_attempt_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    research_run_spec_id bigint NOT NULL,
    attempt_number integer NOT NULL,
    status text NOT NULL DEFAULT 'QUEUED',
    job_id bigint,
    reused_attempt_id bigint,
    result_artifact_id bigint,
    trade_ledger_artifact_id bigint,
    result_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    queued_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    error_message text,
    CONSTRAINT research_run_attempts_spec_fk
        FOREIGN KEY (research_run_spec_id)
        REFERENCES systematic_fx.research_run_specs(research_run_spec_id),
    CONSTRAINT research_run_attempts_job_fk
        FOREIGN KEY (job_id) REFERENCES systematic_fx.jobs(job_id),
    CONSTRAINT research_run_attempts_reused_fk
        FOREIGN KEY (reused_attempt_id)
        REFERENCES systematic_fx.research_run_attempts(research_run_attempt_id),
    CONSTRAINT research_run_attempts_result_artifact_fk
        FOREIGN KEY (result_artifact_id) REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT research_run_attempts_trade_ledger_artifact_fk
        FOREIGN KEY (trade_ledger_artifact_id) REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT research_run_attempts_identity
        UNIQUE (research_run_spec_id, attempt_number),
    CONSTRAINT research_run_attempts_number_positive CHECK (attempt_number > 0),
    CONSTRAINT research_run_attempts_status_valid
        CHECK (status IN
               ('QUEUED', 'RUNNING', 'SUCCEEDED', 'REJECTED', 'FAILED',
                'CANCELLED', 'SKIPPED_DUPLICATE')),
    CONSTRAINT research_run_attempts_result_object
        CHECK (jsonb_typeof(result_summary) = 'object'),
    CONSTRAINT research_run_attempts_running_has_start
        CHECK (status <> 'RUNNING' OR started_at IS NOT NULL),
    CONSTRAINT research_run_attempts_terminal_has_finish
        CHECK (status NOT IN
               ('SUCCEEDED', 'REJECTED', 'FAILED', 'CANCELLED', 'SKIPPED_DUPLICATE')
               OR finished_at IS NOT NULL),
    CONSTRAINT research_run_attempts_success_has_artifact
        CHECK (status <> 'SUCCEEDED' OR result_artifact_id IS NOT NULL),
    CONSTRAINT research_run_attempts_failed_has_error
        CHECK (status <> 'FAILED' OR btrim(COALESCE(error_message, '')) <> ''),
    CONSTRAINT research_run_attempts_skip_reuses_success
        CHECK ((status = 'SKIPPED_DUPLICATE') = (reused_attempt_id IS NOT NULL)),
    CONSTRAINT research_run_attempts_no_self_reuse
        CHECK (reused_attempt_id IS NULL
               OR reused_attempt_id <> research_run_attempt_id),
    CONSTRAINT research_run_attempts_time_order
        CHECK ((started_at IS NULL OR started_at >= queued_at)
               AND (finished_at IS NULL OR finished_at >= queued_at)
               AND (started_at IS NULL OR finished_at IS NULL OR started_at <= finished_at))
);

CREATE INDEX research_run_attempts_status_idx
    ON systematic_fx.research_run_attempts (status, queued_at);
CREATE INDEX research_run_attempts_spec_status_idx
    ON systematic_fx.research_run_attempts (research_run_spec_id, status, attempt_number);
CREATE UNIQUE INDEX research_run_attempts_one_success
    ON systematic_fx.research_run_attempts (research_run_spec_id)
    WHERE status = 'SUCCEEDED';

ALTER TABLE systematic_fx.experiment_trials
    ADD COLUMN research_run_spec_id bigint;

ALTER TABLE systematic_fx.experiment_trials
    ADD CONSTRAINT experiment_trials_run_spec_fk
        FOREIGN KEY (research_run_spec_id)
        REFERENCES systematic_fx.research_run_specs(research_run_spec_id);

CREATE UNIQUE INDEX experiment_trials_parameters_identity
    ON systematic_fx.experiment_trials (experiment_id, parameters_sha256)
    WHERE parameters_sha256 IS NOT NULL;

ALTER TABLE systematic_fx.backtest_runs
    ADD COLUMN research_run_spec_id bigint;

ALTER TABLE systematic_fx.backtest_runs
    ADD CONSTRAINT backtest_runs_run_spec_fk
        FOREIGN KEY (research_run_spec_id)
        REFERENCES systematic_fx.research_run_specs(research_run_spec_id);

ALTER TABLE systematic_fx.discovery_exposures
    ADD COLUMN research_run_spec_id bigint;

ALTER TABLE systematic_fx.discovery_exposures
    ADD CONSTRAINT discovery_exposures_run_spec_fk
        FOREIGN KEY (research_run_spec_id)
        REFERENCES systematic_fx.research_run_specs(research_run_spec_id);

CREATE FUNCTION systematic_fx.reject_research_run_spec_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'research run specifications are immutable';
END;
$$;

CREATE TRIGGER research_run_specs_immutable
BEFORE UPDATE OR DELETE ON systematic_fx.research_run_specs
FOR EACH ROW EXECUTE FUNCTION systematic_fx.reject_research_run_spec_mutation();

CREATE FUNCTION systematic_fx.protect_research_run_attempt_history()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'research run attempts are append-preserved';
    END IF;
    IF OLD.status IN
       ('SUCCEEDED', 'REJECTED', 'FAILED', 'CANCELLED', 'SKIPPED_DUPLICATE') THEN
        RAISE EXCEPTION 'terminal research run attempts are immutable';
    END IF;
    IF NEW.research_run_spec_id <> OLD.research_run_spec_id
       OR NEW.attempt_number <> OLD.attempt_number
       OR NEW.queued_at <> OLD.queued_at THEN
        RAISE EXCEPTION 'research run attempt identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER research_run_attempts_preserve_history
BEFORE UPDATE OR DELETE ON systematic_fx.research_run_attempts
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_research_run_attempt_history();

COMMENT ON TABLE systematic_fx.research_run_specs IS
    'Immutable canonical identity of every performance-bearing or AI-visible research computation.';
COMMENT ON COLUMN systematic_fx.research_run_specs.run_fingerprint IS
    'SHA-256 of strict canonical JSON containing every run variable and versioned input.';
COMMENT ON TABLE systematic_fx.research_run_attempts IS
    'Append-preserved execution attempts; successful fingerprints cannot be executed twice.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (3, 'research_run_ledger', :'migration_checksum');

COMMIT;
