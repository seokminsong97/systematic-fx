BEGIN;

CREATE SCHEMA IF NOT EXISTS systematic_fx;

CREATE TABLE systematic_fx.schema_migrations (
    version integer PRIMARY KEY,
    name text NOT NULL UNIQUE,
    checksum text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT schema_migrations_version_positive CHECK (version > 0),
    CONSTRAINT schema_migrations_checksum_sha256
        CHECK (checksum ~ '^[0-9a-f]{64}$')
);

CREATE TABLE systematic_fx.datasets (
    dataset_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_key text NOT NULL UNIQUE,
    provider text NOT NULL,
    feed text NOT NULL,
    data_schema text NOT NULL,
    root_uri text NOT NULL,
    price_scale_exponent smallint NOT NULL DEFAULT -9,
    status text NOT NULL DEFAULT 'REGISTERED',
    expected_start_date date,
    expected_end_date date,
    manifest_sha256 text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT datasets_key_nonempty CHECK (btrim(dataset_key) <> ''),
    CONSTRAINT datasets_provider_nonempty CHECK (btrim(provider) <> ''),
    CONSTRAINT datasets_feed_nonempty CHECK (btrim(feed) <> ''),
    CONSTRAINT datasets_schema_nonempty CHECK (btrim(data_schema) <> ''),
    CONSTRAINT datasets_root_uri_nonempty CHECK (btrim(root_uri) <> ''),
    CONSTRAINT datasets_price_scale_range CHECK (price_scale_exponent BETWEEN -18 AND 18),
    CONSTRAINT datasets_status_valid
        CHECK (status IN ('REGISTERED', 'VALIDATING', 'READY', 'REJECTED', 'RETIRED')),
    CONSTRAINT datasets_date_order
        CHECK (expected_start_date IS NULL OR expected_end_date IS NULL
               OR expected_start_date <= expected_end_date),
    CONSTRAINT datasets_manifest_sha256_valid
        CHECK (manifest_sha256 IS NULL OR manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT datasets_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE systematic_fx.source_files (
    source_file_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id bigint NOT NULL,
    source_date date NOT NULL,
    relative_uri text NOT NULL,
    byte_size bigint NOT NULL,
    sha256 text,
    row_count bigint,
    parquet_schema_fingerprint text,
    min_event_time_ns bigint,
    max_event_time_ns bigint,
    status text NOT NULL DEFAULT 'DISCOVERED',
    footer_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    discovered_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    validated_at timestamptz,
    CONSTRAINT source_files_dataset_fk
        FOREIGN KEY (dataset_id) REFERENCES systematic_fx.datasets(dataset_id),
    CONSTRAINT source_files_dataset_identity UNIQUE (dataset_id, source_file_id),
    CONSTRAINT source_files_uri_unique UNIQUE (dataset_id, relative_uri),
    CONSTRAINT source_files_uri_nonempty CHECK (btrim(relative_uri) <> ''),
    CONSTRAINT source_files_byte_size_nonnegative CHECK (byte_size >= 0),
    CONSTRAINT source_files_row_count_nonnegative CHECK (row_count IS NULL OR row_count >= 0),
    CONSTRAINT source_files_sha256_valid
        CHECK (sha256 IS NULL OR sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT source_files_schema_fingerprint_valid
        CHECK (parquet_schema_fingerprint IS NULL
               OR parquet_schema_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT source_files_event_time_order
        CHECK (min_event_time_ns IS NULL OR max_event_time_ns IS NULL
               OR min_event_time_ns <= max_event_time_ns),
    CONSTRAINT source_files_status_valid
        CHECK (status IN ('DISCOVERED', 'HASHED', 'VALIDATED', 'REJECTED')),
    CONSTRAINT source_files_validated_has_checksum
        CHECK (status <> 'VALIDATED' OR sha256 IS NOT NULL),
    CONSTRAINT source_files_validation_time_order
        CHECK (validated_at IS NULL OR validated_at >= discovered_at),
    CONSTRAINT source_files_footer_object CHECK (jsonb_typeof(footer_metadata) = 'object')
);

CREATE INDEX source_files_dataset_date_idx
    ON systematic_fx.source_files (dataset_id, source_date);
CREATE INDEX source_files_status_idx
    ON systematic_fx.source_files (dataset_id, status);

CREATE TABLE systematic_fx.instruments (
    instrument_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id bigint NOT NULL,
    instrument_key text NOT NULL,
    provider_symbol text NOT NULL,
    parent_symbol text NOT NULL,
    instrument_class text NOT NULL,
    expiration_date date,
    first_notice_date date,
    last_trade_date date,
    currency text NOT NULL,
    tick_size numeric NOT NULL,
    tick_value numeric NOT NULL,
    contract_multiplier numeric NOT NULL,
    execution_eligible boolean NOT NULL DEFAULT false,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT instruments_dataset_fk
        FOREIGN KEY (dataset_id) REFERENCES systematic_fx.datasets(dataset_id),
    CONSTRAINT instruments_dataset_identity UNIQUE (dataset_id, instrument_id),
    CONSTRAINT instruments_key_unique UNIQUE (dataset_id, instrument_key),
    CONSTRAINT instruments_symbol_unique UNIQUE (dataset_id, provider_symbol),
    CONSTRAINT instruments_key_nonempty CHECK (btrim(instrument_key) <> ''),
    CONSTRAINT instruments_symbol_nonempty CHECK (btrim(provider_symbol) <> ''),
    CONSTRAINT instruments_parent_nonempty CHECK (btrim(parent_symbol) <> ''),
    CONSTRAINT instruments_class_valid
        CHECK (instrument_class IN ('OUTRIGHT', 'CALENDAR_SPREAD', 'OTHER')),
    CONSTRAINT instruments_currency_valid CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT instruments_tick_size_positive CHECK (tick_size > 0),
    CONSTRAINT instruments_tick_value_positive CHECK (tick_value > 0),
    CONSTRAINT instruments_multiplier_positive CHECK (contract_multiplier > 0),
    CONSTRAINT instruments_execution_requires_outright
        CHECK (NOT execution_eligible OR instrument_class = 'OUTRIGHT'),
    CONSTRAINT instruments_notice_before_expiry
        CHECK (first_notice_date IS NULL OR expiration_date IS NULL
               OR first_notice_date <= expiration_date),
    CONSTRAINT instruments_last_trade_before_expiry
        CHECK (last_trade_date IS NULL OR expiration_date IS NULL
               OR last_trade_date <= expiration_date),
    CONSTRAINT instruments_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX instruments_parent_expiry_idx
    ON systematic_fx.instruments (dataset_id, parent_symbol, expiration_date);

CREATE TABLE systematic_fx.instrument_mappings (
    instrument_mapping_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id bigint NOT NULL,
    source_file_id bigint NOT NULL,
    provider_instrument_id bigint NOT NULL,
    instrument_id bigint,
    mapped_symbol text NOT NULL,
    instrument_class text NOT NULL,
    valid_from_date date NOT NULL,
    valid_to_date date NOT NULL,
    mapping_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT instrument_mappings_source_fk
        FOREIGN KEY (dataset_id, source_file_id)
        REFERENCES systematic_fx.source_files(dataset_id, source_file_id),
    CONSTRAINT instrument_mappings_instrument_fk
        FOREIGN KEY (dataset_id, instrument_id)
        REFERENCES systematic_fx.instruments(dataset_id, instrument_id),
    CONSTRAINT instrument_mappings_provider_id_unique
        UNIQUE (source_file_id, provider_instrument_id, valid_from_date),
    CONSTRAINT instrument_mappings_provider_id_nonnegative
        CHECK (provider_instrument_id >= 0),
    CONSTRAINT instrument_mappings_symbol_nonempty CHECK (btrim(mapped_symbol) <> ''),
    CONSTRAINT instrument_mappings_class_valid
        CHECK (instrument_class IN ('OUTRIGHT', 'CALENDAR_SPREAD', 'OTHER')),
    CONSTRAINT instrument_mappings_date_order
        CHECK (valid_from_date < valid_to_date),
    CONSTRAINT instrument_mappings_metadata_object
        CHECK (jsonb_typeof(mapping_metadata) = 'object')
);

CREATE INDEX instrument_mappings_instrument_idx
    ON systematic_fx.instrument_mappings (instrument_id, source_file_id)
    WHERE instrument_id IS NOT NULL;

CREATE TABLE systematic_fx.jobs (
    job_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_key text NOT NULL UNIQUE,
    parent_job_id bigint,
    dataset_id bigint,
    job_type text NOT NULL,
    status text NOT NULL DEFAULT 'QUEUED',
    priority smallint NOT NULL DEFAULT 0,
    idempotency_key text UNIQUE,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    result jsonb NOT NULL DEFAULT '{}'::jsonb,
    attempts integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 1,
    worker_id text,
    leased_until timestamptz,
    queued_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    error_message text,
    CONSTRAINT jobs_parent_fk
        FOREIGN KEY (parent_job_id) REFERENCES systematic_fx.jobs(job_id),
    CONSTRAINT jobs_dataset_fk
        FOREIGN KEY (dataset_id) REFERENCES systematic_fx.datasets(dataset_id),
    CONSTRAINT jobs_key_nonempty CHECK (btrim(job_key) <> ''),
    CONSTRAINT jobs_type_nonempty CHECK (btrim(job_type) <> ''),
    CONSTRAINT jobs_status_valid
        CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    CONSTRAINT jobs_attempts_valid
        CHECK (attempts >= 0 AND max_attempts > 0 AND attempts <= max_attempts),
    CONSTRAINT jobs_priority_range CHECK (priority BETWEEN -100 AND 100),
    CONSTRAINT jobs_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT jobs_result_object CHECK (jsonb_typeof(result) = 'object'),
    CONSTRAINT jobs_running_has_start CHECK (status <> 'RUNNING' OR started_at IS NOT NULL),
    CONSTRAINT jobs_terminal_has_finish
        CHECK (status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED') OR finished_at IS NOT NULL),
    CONSTRAINT jobs_time_order
        CHECK (started_at IS NULL OR finished_at IS NULL OR started_at <= finished_at)
);

CREATE INDEX jobs_claim_idx
    ON systematic_fx.jobs (priority DESC, queued_at)
    WHERE status = 'QUEUED';
CREATE INDEX jobs_status_idx
    ON systematic_fx.jobs (status, job_type);

CREATE TABLE systematic_fx.artifacts (
    artifact_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    artifact_key text NOT NULL UNIQUE,
    artifact_type text NOT NULL,
    uri text NOT NULL UNIQUE,
    sha256 text NOT NULL,
    byte_size bigint NOT NULL,
    media_type text,
    producer_job_id bigint,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT artifacts_producer_job_fk
        FOREIGN KEY (producer_job_id) REFERENCES systematic_fx.jobs(job_id),
    CONSTRAINT artifacts_key_nonempty CHECK (btrim(artifact_key) <> ''),
    CONSTRAINT artifacts_type_nonempty CHECK (btrim(artifact_type) <> ''),
    CONSTRAINT artifacts_uri_nonempty CHECK (btrim(uri) <> ''),
    CONSTRAINT artifacts_sha256_valid CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT artifacts_byte_size_nonnegative CHECK (byte_size >= 0),
    CONSTRAINT artifacts_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX artifacts_type_created_idx
    ON systematic_fx.artifacts (artifact_type, created_at);

CREATE TABLE systematic_fx.derived_partitions (
    derived_partition_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    partition_key text NOT NULL UNIQUE,
    dataset_id bigint NOT NULL,
    instrument_id bigint,
    partition_type text NOT NULL,
    definition_version text NOT NULL,
    source_date date NOT NULL,
    uri text NOT NULL UNIQUE,
    sha256 text NOT NULL,
    row_count bigint NOT NULL,
    min_event_time_ns bigint,
    max_event_time_ns bigint,
    source_manifest_sha256 text NOT NULL,
    code_commit text NOT NULL,
    config_sha256 text NOT NULL,
    manifest_artifact_id bigint,
    build_job_id bigint,
    status text NOT NULL DEFAULT 'BUILDING',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    validated_at timestamptz,
    CONSTRAINT derived_partitions_dataset_fk
        FOREIGN KEY (dataset_id) REFERENCES systematic_fx.datasets(dataset_id),
    CONSTRAINT derived_partitions_instrument_fk
        FOREIGN KEY (dataset_id, instrument_id)
        REFERENCES systematic_fx.instruments(dataset_id, instrument_id),
    CONSTRAINT derived_partitions_manifest_artifact_fk
        FOREIGN KEY (manifest_artifact_id) REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT derived_partitions_build_job_fk
        FOREIGN KEY (build_job_id) REFERENCES systematic_fx.jobs(job_id),
    CONSTRAINT derived_partitions_key_nonempty CHECK (btrim(partition_key) <> ''),
    CONSTRAINT derived_partitions_type_valid
        CHECK (partition_type IN ('FEATURES_1S', 'RESEARCH_5M', 'OUTCOMES', 'OTHER')),
    CONSTRAINT derived_partitions_version_nonempty CHECK (btrim(definition_version) <> ''),
    CONSTRAINT derived_partitions_uri_nonempty CHECK (btrim(uri) <> ''),
    CONSTRAINT derived_partitions_sha256_valid CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT derived_partitions_rows_nonnegative CHECK (row_count >= 0),
    CONSTRAINT derived_partitions_time_order
        CHECK (min_event_time_ns IS NULL OR max_event_time_ns IS NULL
               OR min_event_time_ns <= max_event_time_ns),
    CONSTRAINT derived_partitions_source_manifest_valid
        CHECK (source_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT derived_partitions_config_sha256_valid
        CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT derived_partitions_code_commit_nonempty CHECK (btrim(code_commit) <> ''),
    CONSTRAINT derived_partitions_status_valid
        CHECK (status IN ('BUILDING', 'VALIDATED', 'REJECTED', 'SUPERSEDED')),
    CONSTRAINT derived_partitions_validated_time
        CHECK (validated_at IS NULL OR validated_at >= created_at),
    CONSTRAINT derived_partitions_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX derived_partitions_lookup_idx
    ON systematic_fx.derived_partitions
       (dataset_id, partition_type, definition_version, source_date, instrument_id);

CREATE TABLE systematic_fx.derived_partition_sources (
    derived_partition_id bigint NOT NULL,
    source_file_id bigint NOT NULL,
    source_sha256 text NOT NULL,
    PRIMARY KEY (derived_partition_id, source_file_id),
    CONSTRAINT derived_partition_sources_partition_fk
        FOREIGN KEY (derived_partition_id)
        REFERENCES systematic_fx.derived_partitions(derived_partition_id) ON DELETE CASCADE,
    CONSTRAINT derived_partition_sources_source_fk
        FOREIGN KEY (source_file_id) REFERENCES systematic_fx.source_files(source_file_id),
    CONSTRAINT derived_partition_sources_sha256_valid
        CHECK (source_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX derived_partition_sources_source_idx
    ON systematic_fx.derived_partition_sources (source_file_id);

CREATE TABLE systematic_fx.quality_checks (
    quality_check_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    quality_check_key text NOT NULL UNIQUE,
    dataset_id bigint,
    source_file_id bigint,
    derived_partition_id bigint,
    job_id bigint,
    check_name text NOT NULL,
    checker_version text NOT NULL,
    result text NOT NULL,
    observed jsonb NOT NULL DEFAULT '{}'::jsonb,
    expected jsonb NOT NULL DEFAULT '{}'::jsonb,
    details text,
    checked_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT quality_checks_dataset_fk
        FOREIGN KEY (dataset_id) REFERENCES systematic_fx.datasets(dataset_id),
    CONSTRAINT quality_checks_source_file_fk
        FOREIGN KEY (source_file_id) REFERENCES systematic_fx.source_files(source_file_id),
    CONSTRAINT quality_checks_partition_fk
        FOREIGN KEY (derived_partition_id)
        REFERENCES systematic_fx.derived_partitions(derived_partition_id),
    CONSTRAINT quality_checks_job_fk
        FOREIGN KEY (job_id) REFERENCES systematic_fx.jobs(job_id),
    CONSTRAINT quality_checks_key_nonempty CHECK (btrim(quality_check_key) <> ''),
    CONSTRAINT quality_checks_name_nonempty CHECK (btrim(check_name) <> ''),
    CONSTRAINT quality_checks_version_nonempty CHECK (btrim(checker_version) <> ''),
    CONSTRAINT quality_checks_result_valid
        CHECK (result IN ('PASS', 'WARN', 'FAIL', 'ERROR')),
    CONSTRAINT quality_checks_exactly_one_target
        CHECK (((dataset_id IS NOT NULL)::integer
              + (source_file_id IS NOT NULL)::integer
              + (derived_partition_id IS NOT NULL)::integer) = 1),
    CONSTRAINT quality_checks_observed_object CHECK (jsonb_typeof(observed) = 'object'),
    CONSTRAINT quality_checks_expected_object CHECK (jsonb_typeof(expected) = 'object')
);

CREATE INDEX quality_checks_source_result_idx
    ON systematic_fx.quality_checks (source_file_id, result)
    WHERE source_file_id IS NOT NULL;
CREATE INDEX quality_checks_partition_result_idx
    ON systematic_fx.quality_checks (derived_partition_id, result)
    WHERE derived_partition_id IS NOT NULL;

CREATE TABLE systematic_fx.campaigns (
    campaign_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    campaign_key text NOT NULL UNIQUE,
    dataset_id bigint NOT NULL,
    name text NOT NULL,
    status text NOT NULL DEFAULT 'DRAFT',
    selected_start_date date,
    selected_end_date date,
    roll_cutoff_date date,
    data_manifest_sha256 text,
    feature_version text,
    outcome_version text,
    cost_model_version text,
    execution_model_version text,
    code_commit text NOT NULL,
    config_sha256 text NOT NULL,
    split_policy jsonb NOT NULL,
    trial_budget integer NOT NULL DEFAULT 240,
    finalist_budget integer NOT NULL DEFAULT 10,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    frozen_at timestamptz,
    holdout_revealed_at timestamptz,
    closed_at timestamptz,
    CONSTRAINT campaigns_dataset_fk
        FOREIGN KEY (dataset_id) REFERENCES systematic_fx.datasets(dataset_id),
    CONSTRAINT campaigns_dataset_identity UNIQUE (dataset_id, campaign_id),
    CONSTRAINT campaigns_key_nonempty CHECK (btrim(campaign_key) <> ''),
    CONSTRAINT campaigns_name_nonempty CHECK (btrim(name) <> ''),
    CONSTRAINT campaigns_status_valid
        CHECK (status IN ('DRAFT', 'FROZEN', 'RUNNING', 'CLOSED', 'ABORTED')),
    CONSTRAINT campaigns_date_order
        CHECK (selected_start_date IS NULL OR selected_end_date IS NULL
               OR selected_start_date <= selected_end_date),
    CONSTRAINT campaigns_roll_cutoff_in_range
        CHECK (roll_cutoff_date IS NULL OR selected_end_date IS NULL
               OR roll_cutoff_date <= selected_end_date),
    CONSTRAINT campaigns_manifest_sha256_valid
        CHECK (data_manifest_sha256 IS NULL OR data_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT campaigns_code_commit_nonempty CHECK (btrim(code_commit) <> ''),
    CONSTRAINT campaigns_config_sha256_valid CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT campaigns_split_policy_object CHECK (jsonb_typeof(split_policy) = 'object'),
    CONSTRAINT campaigns_budgets_positive CHECK (trial_budget > 0 AND finalist_budget > 0),
    CONSTRAINT campaigns_finalist_budget_bounded CHECK (finalist_budget <= trial_budget),
    CONSTRAINT campaigns_frozen_time_required
        CHECK (status = 'DRAFT' OR frozen_at IS NOT NULL),
    CONSTRAINT campaigns_reveal_after_freeze
        CHECK (holdout_revealed_at IS NULL OR frozen_at IS NULL
               OR holdout_revealed_at >= frozen_at),
    CONSTRAINT campaigns_close_after_freeze
        CHECK (closed_at IS NULL OR frozen_at IS NULL OR closed_at >= frozen_at)
);

CREATE TABLE systematic_fx.campaign_splits (
    campaign_split_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    campaign_id bigint NOT NULL,
    split_key text NOT NULL,
    split_role text NOT NULL,
    fold_number smallint,
    start_date date NOT NULL,
    end_date date NOT NULL,
    start_active_ordinal integer NOT NULL,
    end_active_ordinal integer NOT NULL,
    purge_before_days integer NOT NULL DEFAULT 0,
    purge_after_days integer NOT NULL DEFAULT 0,
    result_visibility text NOT NULL DEFAULT 'VISIBLE',
    revealed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT campaign_splits_campaign_fk
        FOREIGN KEY (campaign_id) REFERENCES systematic_fx.campaigns(campaign_id),
    CONSTRAINT campaign_splits_campaign_identity UNIQUE (campaign_id, campaign_split_id),
    CONSTRAINT campaign_splits_key_unique UNIQUE (campaign_id, split_key),
    CONSTRAINT campaign_splits_key_nonempty CHECK (btrim(split_key) <> ''),
    CONSTRAINT campaign_splits_role_valid
        CHECK (split_role IN
               ('DISCOVERY', 'WALK_FORWARD', 'EMBARGO', 'HOLDOUT', 'OUTCOME_TAIL')),
    CONSTRAINT campaign_splits_fold_valid
        CHECK ((split_role = 'WALK_FORWARD' AND fold_number BETWEEN 1 AND 5)
               OR (split_role <> 'WALK_FORWARD' AND fold_number IS NULL)),
    CONSTRAINT campaign_splits_date_order CHECK (start_date <= end_date),
    CONSTRAINT campaign_splits_ordinal_order
        CHECK (start_active_ordinal > 0 AND start_active_ordinal <= end_active_ordinal),
    CONSTRAINT campaign_splits_purge_nonnegative
        CHECK (purge_before_days >= 0 AND purge_after_days >= 0),
    CONSTRAINT campaign_splits_visibility_valid
        CHECK (result_visibility IN ('VISIBLE', 'SEALED')),
    CONSTRAINT campaign_splits_reveal_consistent
        CHECK ((result_visibility = 'SEALED' AND revealed_at IS NULL)
               OR result_visibility = 'VISIBLE')
);

CREATE INDEX campaign_splits_dates_idx
    ON systematic_fx.campaign_splits (campaign_id, start_date, end_date);

CREATE TABLE systematic_fx.campaign_days (
    campaign_day_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id bigint NOT NULL,
    campaign_id bigint NOT NULL,
    calendar_date date NOT NULL,
    active_day_ordinal integer,
    eligibility_status text NOT NULL DEFAULT 'PENDING',
    exclusion_reason text,
    campaign_split_id bigint,
    source_file_id bigint,
    execution_instrument_id bigint,
    is_roll_cutoff boolean NOT NULL DEFAULT false,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT campaign_days_campaign_fk
        FOREIGN KEY (dataset_id, campaign_id)
        REFERENCES systematic_fx.campaigns(dataset_id, campaign_id),
    CONSTRAINT campaign_days_split_fk
        FOREIGN KEY (campaign_id, campaign_split_id)
        REFERENCES systematic_fx.campaign_splits(campaign_id, campaign_split_id),
    CONSTRAINT campaign_days_source_fk
        FOREIGN KEY (dataset_id, source_file_id)
        REFERENCES systematic_fx.source_files(dataset_id, source_file_id),
    CONSTRAINT campaign_days_instrument_fk
        FOREIGN KEY (dataset_id, execution_instrument_id)
        REFERENCES systematic_fx.instruments(dataset_id, instrument_id),
    CONSTRAINT campaign_days_date_unique UNIQUE (campaign_id, calendar_date),
    CONSTRAINT campaign_days_eligibility_valid
        CHECK (eligibility_status IN ('PENDING', 'ELIGIBLE', 'INELIGIBLE')),
    CONSTRAINT campaign_days_eligible_has_ordinal
        CHECK (eligibility_status <> 'ELIGIBLE' OR active_day_ordinal IS NOT NULL),
    CONSTRAINT campaign_days_ordinal_positive
        CHECK (active_day_ordinal IS NULL OR active_day_ordinal > 0),
    CONSTRAINT campaign_days_ineligible_not_assigned
        CHECK (eligibility_status <> 'INELIGIBLE' OR campaign_split_id IS NULL),
    CONSTRAINT campaign_days_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX campaign_days_active_ordinal_unique
    ON systematic_fx.campaign_days (campaign_id, active_day_ordinal)
    WHERE active_day_ordinal IS NOT NULL;
CREATE INDEX campaign_days_split_idx
    ON systematic_fx.campaign_days (campaign_split_id, calendar_date)
    WHERE campaign_split_id IS NOT NULL;

CREATE TABLE systematic_fx.pattern_ledger (
    pattern_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pattern_key text NOT NULL UNIQUE,
    campaign_id bigint NOT NULL,
    parent_pattern_id bigint,
    status text NOT NULL DEFAULT 'OPEN',
    first_seen_from timestamptz NOT NULL,
    first_seen_to timestamptz NOT NULL,
    last_updated_interval timestamptz NOT NULL,
    feature_definition_versions jsonb NOT NULL,
    direction text NOT NULL,
    entry_condition text NOT NULL,
    economic_rationale text NOT NULL,
    applicable_regime jsonb NOT NULL DEFAULT '{}'::jsonb,
    counterexamples jsonb NOT NULL DEFAULT '[]'::jsonb,
    support_count bigint NOT NULL DEFAULT 0,
    candidate_barrier_region jsonb NOT NULL DEFAULT '{}'::jsonb,
    forward_first_touch_summaries jsonb NOT NULL DEFAULT '{}'::jsonb,
    cost_assumptions jsonb NOT NULL,
    context_artifact_id bigint,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT pattern_ledger_campaign_fk
        FOREIGN KEY (campaign_id) REFERENCES systematic_fx.campaigns(campaign_id),
    CONSTRAINT pattern_ledger_campaign_identity UNIQUE (campaign_id, pattern_id),
    CONSTRAINT pattern_ledger_parent_fk
        FOREIGN KEY (campaign_id, parent_pattern_id)
        REFERENCES systematic_fx.pattern_ledger(campaign_id, pattern_id),
    CONSTRAINT pattern_ledger_context_artifact_fk
        FOREIGN KEY (context_artifact_id) REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT pattern_ledger_key_nonempty CHECK (btrim(pattern_key) <> ''),
    CONSTRAINT pattern_ledger_status_valid
        CHECK (status IN ('OPEN', 'REGISTERED', 'REJECTED', 'PROMOTED')),
    CONSTRAINT pattern_ledger_interval_order CHECK (first_seen_from <= first_seen_to),
    CONSTRAINT pattern_ledger_update_order CHECK (last_updated_interval >= first_seen_from),
    CONSTRAINT pattern_ledger_feature_versions_container
        CHECK (jsonb_typeof(feature_definition_versions) IN ('object', 'array')),
    CONSTRAINT pattern_ledger_direction_valid
        CHECK (direction IN ('LONG', 'SHORT', 'BOTH', 'NONE')),
    CONSTRAINT pattern_ledger_entry_nonempty CHECK (btrim(entry_condition) <> ''),
    CONSTRAINT pattern_ledger_rationale_nonempty CHECK (btrim(economic_rationale) <> ''),
    CONSTRAINT pattern_ledger_regime_object CHECK (jsonb_typeof(applicable_regime) = 'object'),
    CONSTRAINT pattern_ledger_counterexamples_array CHECK (jsonb_typeof(counterexamples) = 'array'),
    CONSTRAINT pattern_ledger_support_nonnegative CHECK (support_count >= 0),
    CONSTRAINT pattern_ledger_barrier_object
        CHECK (jsonb_typeof(candidate_barrier_region) = 'object'),
    CONSTRAINT pattern_ledger_first_touch_object
        CHECK (jsonb_typeof(forward_first_touch_summaries) = 'object'),
    CONSTRAINT pattern_ledger_cost_object CHECK (jsonb_typeof(cost_assumptions) = 'object')
);

CREATE INDEX pattern_ledger_campaign_status_idx
    ON systematic_fx.pattern_ledger (campaign_id, status, updated_at);

CREATE TABLE systematic_fx.experiments (
    experiment_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    experiment_key text NOT NULL UNIQUE,
    campaign_id bigint NOT NULL,
    pattern_id bigint,
    parent_experiment_id bigint,
    primary_family text NOT NULL,
    status text NOT NULL DEFAULT 'REGISTERED',
    hypothesis text NOT NULL,
    direction text NOT NULL,
    model_family text NOT NULL,
    tick_size numeric NOT NULL,
    tick_value numeric NOT NULL,
    feature_definition_versions jsonb NOT NULL,
    search_boundary jsonb NOT NULL,
    cost_assumptions jsonb NOT NULL,
    execution_assumptions jsonb NOT NULL,
    trial_budget integer NOT NULL,
    trials_registered integer NOT NULL DEFAULT 0,
    registration_artifact_id bigint,
    code_commit text NOT NULL,
    config_sha256 text NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    frozen_at timestamptz,
    completed_at timestamptz,
    CONSTRAINT experiments_campaign_fk
        FOREIGN KEY (campaign_id) REFERENCES systematic_fx.campaigns(campaign_id),
    CONSTRAINT experiments_campaign_identity UNIQUE (campaign_id, experiment_id),
    CONSTRAINT experiments_pattern_fk
        FOREIGN KEY (campaign_id, pattern_id)
        REFERENCES systematic_fx.pattern_ledger(campaign_id, pattern_id),
    CONSTRAINT experiments_parent_fk
        FOREIGN KEY (campaign_id, parent_experiment_id)
        REFERENCES systematic_fx.experiments(campaign_id, experiment_id),
    CONSTRAINT experiments_registration_artifact_fk
        FOREIGN KEY (registration_artifact_id) REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT experiments_key_nonempty CHECK (btrim(experiment_key) <> ''),
    CONSTRAINT experiments_family_nonempty CHECK (btrim(primary_family) <> ''),
    CONSTRAINT experiments_status_valid
        CHECK (status IN
               ('REGISTERED', 'RUNNING', 'REJECTED', 'RETAINED', 'FROZEN', 'FAILED')),
    CONSTRAINT experiments_hypothesis_nonempty CHECK (btrim(hypothesis) <> ''),
    CONSTRAINT experiments_direction_valid
        CHECK (direction IN ('LONG', 'SHORT', 'BOTH')),
    CONSTRAINT experiments_model_family_nonempty CHECK (btrim(model_family) <> ''),
    CONSTRAINT experiments_tick_size_positive CHECK (tick_size > 0),
    CONSTRAINT experiments_tick_value_positive CHECK (tick_value > 0),
    CONSTRAINT experiments_feature_versions_container
        CHECK (jsonb_typeof(feature_definition_versions) IN ('object', 'array')),
    CONSTRAINT experiments_search_boundary_object CHECK (jsonb_typeof(search_boundary) = 'object'),
    CONSTRAINT experiments_cost_object CHECK (jsonb_typeof(cost_assumptions) = 'object'),
    CONSTRAINT experiments_execution_object CHECK (jsonb_typeof(execution_assumptions) = 'object'),
    CONSTRAINT experiments_trial_counts_valid
        CHECK (trial_budget > 0 AND trials_registered >= 0
               AND trials_registered <= trial_budget),
    CONSTRAINT experiments_code_commit_nonempty CHECK (btrim(code_commit) <> ''),
    CONSTRAINT experiments_config_sha256_valid CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT experiments_time_order
        CHECK ((frozen_at IS NULL OR frozen_at >= registered_at)
               AND (completed_at IS NULL OR completed_at >= registered_at))
);

CREATE INDEX experiments_campaign_family_idx
    ON systematic_fx.experiments (campaign_id, primary_family, status);

CREATE TABLE systematic_fx.experiment_trials (
    experiment_trial_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    experiment_id bigint NOT NULL,
    trial_key text NOT NULL,
    trial_type text NOT NULL,
    status text NOT NULL DEFAULT 'REGISTERED',
    parameters jsonb NOT NULL,
    result_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    registered_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    CONSTRAINT experiment_trials_experiment_fk
        FOREIGN KEY (experiment_id)
        REFERENCES systematic_fx.experiments(experiment_id),
    CONSTRAINT experiment_trials_identity UNIQUE (experiment_id, experiment_trial_id),
    CONSTRAINT experiment_trials_key_unique UNIQUE (experiment_id, trial_key),
    CONSTRAINT experiment_trials_key_nonempty CHECK (btrim(trial_key) <> ''),
    CONSTRAINT experiment_trials_type_valid
        CHECK (trial_type IN
               ('STRATEGY_VARIANT', 'BARRIER_CELL', 'MODEL_FIT', 'SCREEN', 'OTHER')),
    CONSTRAINT experiment_trials_status_valid
        CHECK (status IN
               ('REGISTERED', 'RUNNING', 'SUCCEEDED', 'REJECTED', 'FAILED', 'CANCELLED')),
    CONSTRAINT experiment_trials_parameters_object CHECK (jsonb_typeof(parameters) = 'object'),
    CONSTRAINT experiment_trials_result_object CHECK (jsonb_typeof(result_summary) = 'object'),
    CONSTRAINT experiment_trials_running_has_start
        CHECK (status <> 'RUNNING' OR started_at IS NOT NULL),
    CONSTRAINT experiment_trials_terminal_has_finish
        CHECK (status NOT IN ('SUCCEEDED', 'REJECTED', 'FAILED', 'CANCELLED')
               OR finished_at IS NOT NULL),
    CONSTRAINT experiment_trials_time_order
        CHECK (started_at IS NULL OR finished_at IS NULL OR started_at <= finished_at)
);

CREATE INDEX experiment_trials_status_idx
    ON systematic_fx.experiment_trials (experiment_id, status, trial_type);

CREATE TABLE systematic_fx.strategies (
    strategy_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    strategy_key text NOT NULL,
    strategy_version integer NOT NULL,
    campaign_id bigint NOT NULL,
    experiment_id bigint NOT NULL,
    experiment_trial_id bigint,
    parent_strategy_id bigint,
    status text NOT NULL DEFAULT 'DRAFT',
    direction text NOT NULL,
    entry_order_type text NOT NULL,
    entry_policy jsonb NOT NULL,
    take_profit_ticks integer NOT NULL,
    stop_trigger_ticks integer NOT NULL,
    stop_execution_policy jsonb NOT NULL,
    terminal_exit_policy jsonb NOT NULL,
    definition jsonb NOT NULL,
    definition_sha256 text,
    definition_artifact_id bigint,
    feature_version text NOT NULL,
    cost_model_version text NOT NULL,
    execution_model_version text NOT NULL,
    code_commit text NOT NULL,
    config_sha256 text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    frozen_at timestamptz,
    CONSTRAINT strategies_campaign_fk
        FOREIGN KEY (campaign_id) REFERENCES systematic_fx.campaigns(campaign_id),
    CONSTRAINT strategies_campaign_identity UNIQUE (campaign_id, strategy_id),
    CONSTRAINT strategies_experiment_fk
        FOREIGN KEY (campaign_id, experiment_id)
        REFERENCES systematic_fx.experiments(campaign_id, experiment_id),
    CONSTRAINT strategies_trial_fk
        FOREIGN KEY (experiment_id, experiment_trial_id)
        REFERENCES systematic_fx.experiment_trials(experiment_id, experiment_trial_id),
    CONSTRAINT strategies_parent_fk
        FOREIGN KEY (campaign_id, parent_strategy_id)
        REFERENCES systematic_fx.strategies(campaign_id, strategy_id),
    CONSTRAINT strategies_definition_artifact_fk
        FOREIGN KEY (definition_artifact_id) REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT strategies_version_unique UNIQUE (strategy_key, strategy_version),
    CONSTRAINT strategies_key_nonempty CHECK (btrim(strategy_key) <> ''),
    CONSTRAINT strategies_version_positive CHECK (strategy_version > 0),
    CONSTRAINT strategies_status_valid
        CHECK (status IN
               ('DRAFT', 'FROZEN', 'VALIDATED', 'REJECTED', 'PAPER_ELIGIBLE', 'RETIRED')),
    CONSTRAINT strategies_direction_valid CHECK (direction IN ('LONG', 'SHORT')),
    CONSTRAINT strategies_entry_order_valid
        CHECK (entry_order_type IN ('MARKET', 'LIMIT', 'STOP', 'OTHER')),
    CONSTRAINT strategies_take_profit_positive CHECK (take_profit_ticks > 0),
    CONSTRAINT strategies_stop_trigger_positive CHECK (stop_trigger_ticks > 0),
    CONSTRAINT strategies_entry_policy_object CHECK (jsonb_typeof(entry_policy) = 'object'),
    CONSTRAINT strategies_stop_policy_object
        CHECK (jsonb_typeof(stop_execution_policy) = 'object'),
    CONSTRAINT strategies_terminal_policy_object
        CHECK (jsonb_typeof(terminal_exit_policy) = 'object'),
    CONSTRAINT strategies_definition_object CHECK (jsonb_typeof(definition) = 'object'),
    CONSTRAINT strategies_definition_sha256_valid
        CHECK (definition_sha256 IS NULL OR definition_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT strategies_versions_nonempty
        CHECK (btrim(feature_version) <> '' AND btrim(cost_model_version) <> ''
               AND btrim(execution_model_version) <> ''),
    CONSTRAINT strategies_code_commit_nonempty CHECK (btrim(code_commit) <> ''),
    CONSTRAINT strategies_config_sha256_valid CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT strategies_frozen_definition_required
        CHECK (status = 'DRAFT'
               OR (frozen_at IS NOT NULL AND definition_sha256 IS NOT NULL)),
    CONSTRAINT strategies_frozen_time_order
        CHECK (frozen_at IS NULL OR frozen_at >= created_at)
);

CREATE INDEX strategies_campaign_status_idx
    ON systematic_fx.strategies (campaign_id, status);
CREATE INDEX strategies_experiment_idx
    ON systematic_fx.strategies (experiment_id, strategy_version);

CREATE TABLE systematic_fx.backtest_runs (
    backtest_run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_key text NOT NULL UNIQUE,
    campaign_id bigint NOT NULL,
    strategy_id bigint NOT NULL,
    campaign_split_id bigint,
    experiment_trial_id bigint,
    parent_run_id bigint,
    job_id bigint,
    run_type text NOT NULL,
    status text NOT NULL DEFAULT 'QUEUED',
    deterministic_seed bigint NOT NULL,
    engine_version text NOT NULL,
    input_manifest_sha256 text NOT NULL,
    code_commit text NOT NULL,
    config_sha256 text NOT NULL,
    cost_model_version text NOT NULL,
    execution_model_version text NOT NULL,
    stress_scenario jsonb NOT NULL DEFAULT '{}'::jsonb,
    result_artifact_id bigint,
    trade_ledger_artifact_id bigint,
    queued_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    error_message text,
    CONSTRAINT backtest_runs_campaign_fk
        FOREIGN KEY (campaign_id) REFERENCES systematic_fx.campaigns(campaign_id),
    CONSTRAINT backtest_runs_campaign_identity UNIQUE (campaign_id, backtest_run_id),
    CONSTRAINT backtest_runs_strategy_fk
        FOREIGN KEY (campaign_id, strategy_id)
        REFERENCES systematic_fx.strategies(campaign_id, strategy_id),
    CONSTRAINT backtest_runs_split_fk
        FOREIGN KEY (campaign_id, campaign_split_id)
        REFERENCES systematic_fx.campaign_splits(campaign_id, campaign_split_id),
    CONSTRAINT backtest_runs_trial_fk
        FOREIGN KEY (experiment_trial_id)
        REFERENCES systematic_fx.experiment_trials(experiment_trial_id),
    CONSTRAINT backtest_runs_parent_fk
        FOREIGN KEY (campaign_id, parent_run_id)
        REFERENCES systematic_fx.backtest_runs(campaign_id, backtest_run_id),
    CONSTRAINT backtest_runs_job_fk
        FOREIGN KEY (job_id) REFERENCES systematic_fx.jobs(job_id),
    CONSTRAINT backtest_runs_result_artifact_fk
        FOREIGN KEY (result_artifact_id) REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT backtest_runs_trade_ledger_artifact_fk
        FOREIGN KEY (trade_ledger_artifact_id) REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT backtest_runs_key_nonempty CHECK (btrim(run_key) <> ''),
    CONSTRAINT backtest_runs_type_valid
        CHECK (run_type IN
               ('DISCOVERY', 'SCREEN', 'WALK_FORWARD', 'HOLDOUT', 'STRESS', 'PILOT')),
    CONSTRAINT backtest_runs_status_valid
        CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    CONSTRAINT backtest_runs_engine_version_nonempty CHECK (btrim(engine_version) <> ''),
    CONSTRAINT backtest_runs_input_manifest_valid
        CHECK (input_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT backtest_runs_code_commit_nonempty CHECK (btrim(code_commit) <> ''),
    CONSTRAINT backtest_runs_config_sha256_valid CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT backtest_runs_model_versions_nonempty
        CHECK (btrim(cost_model_version) <> '' AND btrim(execution_model_version) <> ''),
    CONSTRAINT backtest_runs_stress_object CHECK (jsonb_typeof(stress_scenario) = 'object'),
    CONSTRAINT backtest_runs_running_has_start
        CHECK (status <> 'RUNNING' OR started_at IS NOT NULL),
    CONSTRAINT backtest_runs_terminal_has_finish
        CHECK (status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED') OR finished_at IS NOT NULL),
    CONSTRAINT backtest_runs_success_has_artifact
        CHECK (status <> 'SUCCEEDED' OR result_artifact_id IS NOT NULL),
    CONSTRAINT backtest_runs_time_order
        CHECK (started_at IS NULL OR finished_at IS NULL OR started_at <= finished_at)
);

CREATE INDEX backtest_runs_strategy_status_idx
    ON systematic_fx.backtest_runs (strategy_id, status, run_type);
CREATE INDEX backtest_runs_split_idx
    ON systematic_fx.backtest_runs (campaign_split_id, run_type)
    WHERE campaign_split_id IS NOT NULL;

CREATE TABLE systematic_fx.backtest_metrics (
    backtest_metric_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    backtest_run_id bigint NOT NULL,
    metric_scope text NOT NULL,
    scope_key text NOT NULL DEFAULT 'ALL',
    metric_name text NOT NULL,
    metric_value numeric,
    metric_json jsonb,
    unit text,
    sample_count bigint,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT backtest_metrics_run_fk
        FOREIGN KEY (backtest_run_id)
        REFERENCES systematic_fx.backtest_runs(backtest_run_id) ON DELETE CASCADE,
    CONSTRAINT backtest_metrics_identity_unique
        UNIQUE (backtest_run_id, metric_scope, scope_key, metric_name),
    CONSTRAINT backtest_metrics_scope_valid
        CHECK (metric_scope IN
               ('AGGREGATE', 'FOLD', 'DAY', 'REGIME', 'DIRECTION', 'BARRIER')),
    CONSTRAINT backtest_metrics_scope_key_nonempty CHECK (btrim(scope_key) <> ''),
    CONSTRAINT backtest_metrics_name_nonempty CHECK (btrim(metric_name) <> ''),
    CONSTRAINT backtest_metrics_exactly_one_value
        CHECK ((metric_value IS NOT NULL) <> (metric_json IS NOT NULL)),
    CONSTRAINT backtest_metrics_json_container
        CHECK (metric_json IS NULL OR jsonb_typeof(metric_json) IN ('object', 'array')),
    CONSTRAINT backtest_metrics_sample_count_nonnegative
        CHECK (sample_count IS NULL OR sample_count >= 0)
);

CREATE INDEX backtest_metrics_name_idx
    ON systematic_fx.backtest_metrics (metric_name, metric_scope);

COMMENT ON SCHEMA systematic_fx IS
    'Research control plane. Raw market events and wide feature rows remain in immutable Parquet.';
COMMENT ON TABLE systematic_fx.source_files IS
    'Immutable source-file catalog; no MBP-10 event rows are stored here.';
COMMENT ON TABLE systematic_fx.derived_partition_sources IS
    'Many-to-many checksum lineage from derived Parquet partitions to immutable source files.';
COMMENT ON TABLE systematic_fx.campaign_splits IS
    'Performance-independent chronological split boundaries, including sealed holdout periods.';
COMMENT ON TABLE systematic_fx.pattern_ledger IS
    'Durable AI discovery observations, counterexamples, and promotion/rejection state.';
COMMENT ON TABLE systematic_fx.experiment_trials IS
    'Complete multiplicity ledger, including failed and rejected barrier cells and variants.';
COMMENT ON TABLE systematic_fx.strategies IS
    'Versioned executable bracket policies with integer-tick take-profit and stop distances.';
COMMENT ON TABLE systematic_fx.backtest_metrics IS
    'Compact run metrics only; full trade ledgers are immutable artifacts.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (1, 'research_control_plane', :'migration_checksum');

COMMIT;
