BEGIN;

ALTER TABLE systematic_fx.research_run_specs
    ADD CONSTRAINT research_run_specs_outcome_identity
        UNIQUE (research_run_spec_id, campaign_id, run_fingerprint);

ALTER TABLE systematic_fx.research_run_attempts
    ADD CONSTRAINT research_run_attempts_outcome_identity
        UNIQUE (research_run_attempt_id, research_run_spec_id);

CREATE TABLE systematic_fx.phase1a_outcome_replay_manifests (
    outcome_replay_manifest_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    research_run_spec_id bigint NOT NULL,
    research_run_attempt_id bigint NOT NULL UNIQUE,
    campaign_id bigint NOT NULL,
    run_fingerprint text NOT NULL,
    pattern_key text NOT NULL,
    source_artifact_manifest_sha256 text NOT NULL,
    source_slice_count smallint NOT NULL,
    source_occurrence_count integer NOT NULL,
    scenario_count smallint NOT NULL,
    direction_count smallint NOT NULL,
    barrier_axis_size smallint NOT NULL,
    cell_count_per_surface integer NOT NULL,
    expected_summary_count integer NOT NULL,
    status text NOT NULL DEFAULT 'QUEUED',
    result_artifact_id bigint,
    result_artifact_sha256 text,
    result_artifact_byte_size bigint,
    cell_summaries_sha256 text,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    error_message text,
    CONSTRAINT phase1a_outcome_manifests_run_spec_fk
        FOREIGN KEY (research_run_spec_id, campaign_id, run_fingerprint)
        REFERENCES systematic_fx.research_run_specs
            (research_run_spec_id, campaign_id, run_fingerprint),
    CONSTRAINT phase1a_outcome_manifests_attempt_fk
        FOREIGN KEY (research_run_attempt_id, research_run_spec_id)
        REFERENCES systematic_fx.research_run_attempts
            (research_run_attempt_id, research_run_spec_id),
    CONSTRAINT phase1a_outcome_manifests_artifact_fk
        FOREIGN KEY (result_artifact_id)
        REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT phase1a_outcome_manifests_fingerprint_valid
        CHECK (run_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT phase1a_outcome_manifests_source_sha256_valid
        CHECK (source_artifact_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT phase1a_outcome_manifests_result_sha256_valid
        CHECK (result_artifact_sha256 IS NULL
               OR result_artifact_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT phase1a_outcome_manifests_cells_sha256_valid
        CHECK (cell_summaries_sha256 IS NULL
               OR cell_summaries_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT phase1a_outcome_manifests_fixed_identity
        CHECK (pattern_key = 'p5_01_range_expansion_flow_continuation'
               AND source_slice_count = 99
               AND source_occurrence_count = 1111
               AND scenario_count = 3
               AND direction_count = 2
               AND barrier_axis_size = 22
               AND cell_count_per_surface = 484
               AND expected_summary_count = 2904),
    CONSTRAINT phase1a_outcome_manifests_status_valid
        CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT phase1a_outcome_manifests_running_has_start
        CHECK (status <> 'RUNNING' OR started_at IS NOT NULL),
    CONSTRAINT phase1a_outcome_manifests_terminal_has_finish
        CHECK (status NOT IN ('SUCCEEDED', 'FAILED') OR finished_at IS NOT NULL),
    CONSTRAINT phase1a_outcome_manifests_success_result_complete
        CHECK ((status = 'SUCCEEDED') =
               (result_artifact_id IS NOT NULL
                AND result_artifact_sha256 IS NOT NULL
                AND result_artifact_byte_size IS NOT NULL
                AND cell_summaries_sha256 IS NOT NULL)),
    CONSTRAINT phase1a_outcome_manifests_result_size_positive
        CHECK (result_artifact_byte_size IS NULL OR result_artifact_byte_size > 0),
    CONSTRAINT phase1a_outcome_manifests_failed_has_error
        CHECK (status <> 'FAILED' OR btrim(COALESCE(error_message, '')) <> ''),
    CONSTRAINT phase1a_outcome_manifests_nonfailed_has_no_error
        CHECK (status = 'FAILED' OR error_message IS NULL),
    CONSTRAINT phase1a_outcome_manifests_time_order
        CHECK ((started_at IS NULL OR started_at >= created_at)
               AND (finished_at IS NULL OR finished_at >= created_at)
               AND (started_at IS NULL OR finished_at IS NULL
                    OR started_at <= finished_at)),
    CONSTRAINT phase1a_outcome_manifests_id_fingerprint_unique
        UNIQUE (outcome_replay_manifest_id, run_fingerprint)
);

CREATE INDEX phase1a_outcome_manifests_spec_status_idx
    ON systematic_fx.phase1a_outcome_replay_manifests
        (research_run_spec_id, status, created_at);

CREATE TABLE systematic_fx.phase1a_outcome_replay_checkpoints (
    outcome_replay_manifest_id bigint NOT NULL,
    checkpoint_sequence bigint NOT NULL,
    run_fingerprint text NOT NULL,
    completed_source_date_count integer NOT NULL,
    last_completed_source_date date NOT NULL,
    source_event_count bigint NOT NULL,
    checkpoint_artifact_id bigint NOT NULL UNIQUE,
    checkpoint_artifact_sha256 text NOT NULL,
    checkpoint_artifact_byte_size bigint NOT NULL,
    predecessor_checkpoint_sha256 text,
    progress_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (outcome_replay_manifest_id, checkpoint_sequence),
    CONSTRAINT phase1a_outcome_checkpoints_manifest_fk
        FOREIGN KEY (outcome_replay_manifest_id, run_fingerprint)
        REFERENCES systematic_fx.phase1a_outcome_replay_manifests
            (outcome_replay_manifest_id, run_fingerprint),
    CONSTRAINT phase1a_outcome_checkpoints_artifact_fk
        FOREIGN KEY (checkpoint_artifact_id)
        REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT phase1a_outcome_checkpoints_fingerprint_valid
        CHECK (run_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT phase1a_outcome_checkpoints_progress_valid
        CHECK (checkpoint_sequence > 0
               AND completed_source_date_count > 0
               AND source_event_count >= 0),
    CONSTRAINT phase1a_outcome_checkpoints_artifact_sha256_valid
        CHECK (checkpoint_artifact_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT phase1a_outcome_checkpoints_artifact_size_positive
        CHECK (checkpoint_artifact_byte_size > 0),
    CONSTRAINT phase1a_outcome_checkpoints_predecessor_valid
        CHECK ((checkpoint_sequence = 1) =
               (predecessor_checkpoint_sha256 IS NULL)
               AND (predecessor_checkpoint_sha256 IS NULL
                    OR predecessor_checkpoint_sha256 ~ '^[0-9a-f]{64}$')),
    CONSTRAINT phase1a_outcome_checkpoints_metadata_object
        CHECK (jsonb_typeof(progress_metadata) = 'object')
);

CREATE TABLE systematic_fx.phase1a_outcome_cell_summaries (
    outcome_replay_manifest_id bigint NOT NULL,
    run_fingerprint text NOT NULL,
    scenario_id text NOT NULL,
    direction text NOT NULL,
    take_profit_ticks integer NOT NULL,
    stop_loss_ticks integer NOT NULL,
    signal_count bigint NOT NULL,
    entry_fill_count bigint NOT NULL,
    entry_not_filled_count bigint NOT NULL,
    skipped_occupied_count bigint NOT NULL,
    take_profit_first_count bigint NOT NULL,
    stop_first_count bigint NOT NULL,
    terminal_exit_count bigint NOT NULL,
    censored_count bigint NOT NULL,
    gross_pnl_ticks bigint NOT NULL,
    variable_cost_ticks bigint NOT NULL,
    allocated_fixed_cost_ticks bigint NOT NULL,
    fully_loaded_net_pnl_ticks bigint NOT NULL,
    fully_loaded_net_ev_ticks numeric,
    fully_loaded_net_pnl_usd numeric NOT NULL,
    calendar_month_net_pnl_usd numeric NOT NULL,
    profit_factor numeric,
    maximum_drawdown_usd numeric NOT NULL,
    complete boolean NOT NULL,
    summary_sha256 text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (
        outcome_replay_manifest_id,
        scenario_id,
        direction,
        take_profit_ticks,
        stop_loss_ticks
    ),
    CONSTRAINT phase1a_outcome_cells_manifest_fk
        FOREIGN KEY (outcome_replay_manifest_id, run_fingerprint)
        REFERENCES systematic_fx.phase1a_outcome_replay_manifests
            (outcome_replay_manifest_id, run_fingerprint),
    CONSTRAINT phase1a_outcome_cells_fingerprint_valid
        CHECK (run_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT phase1a_outcome_cells_scenario_valid
        CHECK (scenario_id IN
               ('BASELINE', 'MODERATE_COMBINED', 'SEVERE_DIAGNOSTIC')),
    CONSTRAINT phase1a_outcome_cells_direction_valid
        CHECK (direction IN ('LONG', 'SHORT')),
    CONSTRAINT phase1a_outcome_cells_take_profit_grid_valid
        CHECK (take_profit_ticks BETWEEN 24 AND 192
               AND (take_profit_ticks - 24) % 8 = 0),
    CONSTRAINT phase1a_outcome_cells_stop_loss_grid_valid
        CHECK (stop_loss_ticks BETWEEN 24 AND 192
               AND (stop_loss_ticks - 24) % 8 = 0),
    CONSTRAINT phase1a_outcome_cells_counts_nonnegative
        CHECK (signal_count >= 0
               AND entry_fill_count >= 0
               AND entry_not_filled_count >= 0
               AND skipped_occupied_count >= 0
               AND take_profit_first_count >= 0
               AND stop_first_count >= 0
               AND terminal_exit_count >= 0
               AND censored_count >= 0
               AND variable_cost_ticks >= 0
               AND allocated_fixed_cost_ticks >= 0),
    CONSTRAINT phase1a_outcome_cells_signal_accounting
        CHECK (signal_count = entry_fill_count
                              + entry_not_filled_count
                              + skipped_occupied_count),
    CONSTRAINT phase1a_outcome_cells_outcome_accounting
        CHECK (entry_fill_count = take_profit_first_count
                                  + stop_first_count
                                  + terminal_exit_count
                                  + censored_count),
    CONSTRAINT phase1a_outcome_cells_net_tick_accounting
        CHECK (fully_loaded_net_pnl_ticks = gross_pnl_ticks
                                              - variable_cost_ticks
                                              - allocated_fixed_cost_ticks),
    CONSTRAINT phase1a_outcome_cells_complete_consistent
        CHECK (complete = (censored_count = 0)),
    CONSTRAINT phase1a_outcome_cells_decimal_metrics_valid
        CHECK ((fully_loaded_net_ev_ticks IS NULL
                OR fully_loaded_net_ev_ticks = fully_loaded_net_ev_ticks)
               AND (profit_factor IS NULL OR profit_factor >= 0)
               AND maximum_drawdown_usd >= 0),
    CONSTRAINT phase1a_outcome_cells_sha256_valid
        CHECK (summary_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX phase1a_outcome_cells_surface_idx
    ON systematic_fx.phase1a_outcome_cell_summaries
        (outcome_replay_manifest_id, scenario_id, direction);

CREATE FUNCTION systematic_fx.protect_phase1a_outcome_manifest()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    campaign_key_value text;
    spec_kind text;
    spec_engine text;
    spec_direction text;
    spec_experiment_id bigint;
    spec_parameters jsonb;
    attempt_status text;
    attempt_artifact_id bigint;
    attempt_started_at timestamptz;
    attempt_finished_at timestamptz;
    attempt_error text;
    attempt_summary jsonb;
    artifact_type_value text;
    artifact_uri_value text;
    artifact_sha256_value text;
    artifact_byte_size_value bigint;
    artifact_metadata_value jsonb;
    observed_summary_count integer;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Phase 1A outcome replay manifests are append-preserved';
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF OLD.status IN ('SUCCEEDED', 'FAILED') THEN
            RAISE EXCEPTION 'terminal Phase 1A outcome replay manifests are immutable';
        END IF;
        IF NEW.outcome_replay_manifest_id IS DISTINCT FROM OLD.outcome_replay_manifest_id
           OR NEW.research_run_spec_id IS DISTINCT FROM OLD.research_run_spec_id
           OR NEW.research_run_attempt_id IS DISTINCT FROM OLD.research_run_attempt_id
           OR NEW.campaign_id IS DISTINCT FROM OLD.campaign_id
           OR NEW.run_fingerprint IS DISTINCT FROM OLD.run_fingerprint
           OR NEW.pattern_key IS DISTINCT FROM OLD.pattern_key
           OR NEW.source_artifact_manifest_sha256
                IS DISTINCT FROM OLD.source_artifact_manifest_sha256
           OR NEW.source_slice_count IS DISTINCT FROM OLD.source_slice_count
           OR NEW.source_occurrence_count IS DISTINCT FROM OLD.source_occurrence_count
           OR NEW.scenario_count IS DISTINCT FROM OLD.scenario_count
           OR NEW.direction_count IS DISTINCT FROM OLD.direction_count
           OR NEW.barrier_axis_size IS DISTINCT FROM OLD.barrier_axis_size
           OR NEW.cell_count_per_surface IS DISTINCT FROM OLD.cell_count_per_surface
           OR NEW.expected_summary_count IS DISTINCT FROM OLD.expected_summary_count
           OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
            RAISE EXCEPTION 'Phase 1A outcome replay manifest identity is immutable';
        END IF;
        IF NOT (
            (OLD.status = 'QUEUED' AND NEW.status IN ('RUNNING', 'FAILED'))
            OR (OLD.status = 'RUNNING' AND NEW.status IN ('SUCCEEDED', 'FAILED'))
        ) THEN
            RAISE EXCEPTION 'invalid Phase 1A outcome replay status transition';
        END IF;
    END IF;

    SELECT campaign.campaign_key,
           run_spec.run_kind,
           run_spec.engine_version,
           run_spec.direction,
           run_spec.experiment_id,
           run_spec.canonical_spec -> 'parameters'
    INTO STRICT campaign_key_value,
                spec_kind,
                spec_engine,
                spec_direction,
                spec_experiment_id,
                spec_parameters
    FROM systematic_fx.research_run_specs AS run_spec
    JOIN systematic_fx.campaigns AS campaign
      ON campaign.campaign_id = run_spec.campaign_id
    WHERE run_spec.research_run_spec_id = NEW.research_run_spec_id
      AND run_spec.campaign_id = NEW.campaign_id
      AND run_spec.run_fingerprint = NEW.run_fingerprint;

    IF campaign_key_value <> 'phase1a_conservative_screening_v1'
       OR spec_kind <> 'OUTCOME_BUILD'
       OR spec_engine <> 'phase1a_shared_outcome_replay_v1'
       OR spec_direction <> 'BOTH'
       OR spec_experiment_id IS NOT NULL THEN
        RAISE EXCEPTION 'outcome replay must belong to the campaign-level Phase 1A p5 RunSpec';
    END IF;
    IF spec_parameters #>> '{query_id}' <>
            'p5_01_range_expansion_flow_continuation'
       OR spec_parameters #>> '{outcome_config_id}' <>
            'phase1a_p5_outcome_replay_v1'
       OR spec_parameters #>> '{source_artifact_manifest_sha256}'
            <> NEW.source_artifact_manifest_sha256
       OR spec_parameters #>> '{source_slice_count}' <> '99'
       OR spec_parameters #>> '{source_occurrence_count}' <> '1111'
       OR spec_parameters #>> '{cell_count_per_surface}' <> '484'
       OR spec_parameters #>> '{expected_summary_count}' <> '2904'
       OR spec_parameters #> '{scenario_ids}' <>
            '["BASELINE", "MODERATE_COMBINED", "SEVERE_DIAGNOSTIC"]'::jsonb
       OR spec_parameters #> '{direction_ids}' <> '["LONG", "SHORT"]'::jsonb
       OR spec_parameters #> '{take_profit_ticks}' <>
            '[24,32,40,48,56,64,72,80,88,96,104,112,120,128,136,144,152,160,168,176,184,192]'::jsonb
       OR spec_parameters #> '{stop_loss_ticks}' <>
            '[24,32,40,48,56,64,72,80,88,96,104,112,120,128,136,144,152,160,168,176,184,192]'::jsonb THEN
        RAISE EXCEPTION 'Phase 1A p5 outcome RunSpec parameter drift';
    END IF;

    SELECT status, result_artifact_id, started_at, finished_at, error_message, result_summary
    INTO STRICT attempt_status,
                attempt_artifact_id,
                attempt_started_at,
                attempt_finished_at,
                attempt_error,
                attempt_summary
    FROM systematic_fx.research_run_attempts
    WHERE research_run_attempt_id = NEW.research_run_attempt_id
      AND research_run_spec_id = NEW.research_run_spec_id;

    IF attempt_status <> NEW.status
       OR attempt_artifact_id IS DISTINCT FROM NEW.result_artifact_id
       OR attempt_started_at IS DISTINCT FROM NEW.started_at
       OR attempt_finished_at IS DISTINCT FROM NEW.finished_at
       OR attempt_error IS DISTINCT FROM NEW.error_message THEN
        RAISE EXCEPTION 'outcome replay manifest and run attempt state differ';
    END IF;

    IF NEW.status = 'SUCCEEDED' THEN
        SELECT artifact_type, uri, sha256, byte_size, metadata
        INTO STRICT artifact_type_value,
                    artifact_uri_value,
                    artifact_sha256_value,
                    artifact_byte_size_value,
                    artifact_metadata_value
        FROM systematic_fx.artifacts
        WHERE artifact_id = NEW.result_artifact_id;

        IF artifact_type_value <> 'PHASE1A_OUTCOME_REPLAY_RESULT'
           OR position('/data/derived/' IN artifact_uri_value) = 0
           OR artifact_sha256_value <> NEW.result_artifact_sha256
           OR artifact_byte_size_value <> NEW.result_artifact_byte_size
           OR artifact_metadata_value #>> '{campaign_key}' <>
                'phase1a_conservative_screening_v1'
           OR artifact_metadata_value #>> '{query_id}' <>
                'p5_01_range_expansion_flow_continuation'
           OR artifact_metadata_value #>> '{outcome_config_id}' <>
                'phase1a_p5_outcome_replay_v1'
           OR artifact_metadata_value #>> '{run_fingerprint}' <> NEW.run_fingerprint
           OR artifact_metadata_value #>> '{source_artifact_manifest_sha256}' <>
                NEW.source_artifact_manifest_sha256
           OR artifact_metadata_value #>> '{cell_summaries_sha256}' <>
                NEW.cell_summaries_sha256
           OR artifact_metadata_value #>> '{summary_row_count}' <> '2904' THEN
            RAISE EXCEPTION 'Phase 1A outcome replay result artifact lineage drift';
        END IF;

        SELECT count(*)::integer
        INTO observed_summary_count
        FROM systematic_fx.phase1a_outcome_cell_summaries
        WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
          AND run_fingerprint = NEW.run_fingerprint;
        IF observed_summary_count <> NEW.expected_summary_count THEN
            RAISE EXCEPTION 'Phase 1A outcome replay requires all 2904 cell summaries';
        END IF;
        IF attempt_summary #>> '{artifact_sha256}' <> NEW.result_artifact_sha256
           OR attempt_summary #>> '{cell_summaries_sha256}' <> NEW.cell_summaries_sha256
           OR attempt_summary #>> '{summary_row_count}' <> '2904'
           OR attempt_summary #>> '{source_artifact_manifest_sha256}' <>
                NEW.source_artifact_manifest_sha256 THEN
            RAISE EXCEPTION 'Phase 1A outcome replay attempt summary drift';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER phase1a_outcome_manifests_preserve_and_validate
BEFORE INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_outcome_replay_manifests
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_phase1a_outcome_manifest();

CREATE FUNCTION systematic_fx.protect_phase1a_outcome_checkpoint()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    manifest_fingerprint text;
    manifest_status text;
    previous_sequence bigint;
    previous_source_date_count integer;
    previous_source_date date;
    previous_source_event_count bigint;
    previous_artifact_sha256 text;
    artifact_type_value text;
    artifact_uri_value text;
    artifact_sha256_value text;
    artifact_byte_size_value bigint;
    artifact_metadata_value jsonb;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Phase 1A outcome replay checkpoints are append-only';
    END IF;
    SELECT run_fingerprint, status
    INTO STRICT manifest_fingerprint, manifest_status
    FROM systematic_fx.phase1a_outcome_replay_manifests
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id;
    IF manifest_fingerprint <> NEW.run_fingerprint THEN
        RAISE EXCEPTION 'checkpoint run fingerprint differs from its outcome replay';
    END IF;
    IF manifest_status <> 'RUNNING' THEN
        RAISE EXCEPTION 'checkpoints may be recorded only for a RUNNING outcome replay';
    END IF;
    SELECT checkpoint_sequence, completed_source_date_count,
           last_completed_source_date, source_event_count,
           checkpoint_artifact_sha256
    INTO previous_sequence, previous_source_date_count, previous_source_date,
         previous_source_event_count, previous_artifact_sha256
    FROM systematic_fx.phase1a_outcome_replay_checkpoints
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
    ORDER BY checkpoint_sequence DESC
    LIMIT 1;
    IF NOT FOUND THEN
        IF NEW.checkpoint_sequence <> 1
           OR NEW.completed_source_date_count <> 1
           OR NEW.predecessor_checkpoint_sha256 IS NOT NULL THEN
            RAISE EXCEPTION 'first outcome checkpoint must be source-date sequence 1';
        END IF;
    ELSIF NEW.checkpoint_sequence <> previous_sequence + 1
          OR NEW.completed_source_date_count <> previous_source_date_count + 1
          OR NEW.last_completed_source_date <= previous_source_date
          OR NEW.source_event_count < previous_source_event_count
          OR NEW.predecessor_checkpoint_sha256 <> previous_artifact_sha256 THEN
        RAISE EXCEPTION 'outcome replay checkpoints must form one monotonic hash chain';
    END IF;

    SELECT artifact_type, uri, sha256, byte_size, metadata
    INTO STRICT artifact_type_value, artifact_uri_value, artifact_sha256_value,
                artifact_byte_size_value, artifact_metadata_value
    FROM systematic_fx.artifacts
    WHERE artifact_id = NEW.checkpoint_artifact_id;
    IF artifact_type_value <> 'PHASE1A_OUTCOME_REPLAY_CHECKPOINT'
       OR position('/data/derived/' IN artifact_uri_value) = 0
       OR artifact_sha256_value <> NEW.checkpoint_artifact_sha256
       OR artifact_byte_size_value <> NEW.checkpoint_artifact_byte_size
       OR artifact_metadata_value #>> '{run_fingerprint}' <> NEW.run_fingerprint
       OR artifact_metadata_value #>> '{checkpoint_sequence}' <>
            NEW.checkpoint_sequence::text
       OR artifact_metadata_value #>> '{last_completed_source_date}' <>
            NEW.last_completed_source_date::text THEN
        RAISE EXCEPTION 'outcome replay checkpoint artifact lineage drift';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER phase1a_outcome_checkpoints_preserve_progress
BEFORE INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_outcome_replay_checkpoints
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_phase1a_outcome_checkpoint();

CREATE FUNCTION systematic_fx.protect_phase1a_outcome_cell_summary()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    manifest_fingerprint text;
    manifest_status text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Phase 1A outcome cell summaries are append-only';
    END IF;
    SELECT run_fingerprint, status
    INTO STRICT manifest_fingerprint, manifest_status
    FROM systematic_fx.phase1a_outcome_replay_manifests
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id;
    IF manifest_fingerprint <> NEW.run_fingerprint OR manifest_status <> 'RUNNING' THEN
        RAISE EXCEPTION 'outcome cell summary must belong to its RUNNING replay fingerprint';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER phase1a_outcome_cells_append_only
BEFORE INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_outcome_cell_summaries
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_phase1a_outcome_cell_summary();

CREATE FUNCTION systematic_fx.require_phase1a_outcome_attempt_manifest()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    governed boolean;
    manifest_status text;
    manifest_artifact_id bigint;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM systematic_fx.research_run_specs AS run_spec
        JOIN systematic_fx.campaigns AS campaign
          ON campaign.campaign_id = run_spec.campaign_id
        WHERE run_spec.research_run_spec_id = NEW.research_run_spec_id
          AND campaign.campaign_key = 'phase1a_conservative_screening_v1'
          AND run_spec.run_kind = 'OUTCOME_BUILD'
          AND run_spec.engine_version = 'phase1a_shared_outcome_replay_v1'
          AND run_spec.canonical_spec #>> '{parameters,query_id}' =
              'p5_01_range_expansion_flow_continuation'
    ) INTO governed;
    IF NOT governed OR NEW.status = 'SKIPPED_DUPLICATE' THEN
        RETURN NULL;
    END IF;
    IF NEW.status NOT IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED') THEN
        RAISE EXCEPTION 'governed Phase 1A outcome attempts use QUEUED/RUNNING/SUCCEEDED/FAILED';
    END IF;
    SELECT status, result_artifact_id
    INTO manifest_status, manifest_artifact_id
    FROM systematic_fx.phase1a_outcome_replay_manifests
    WHERE research_run_attempt_id = NEW.research_run_attempt_id
      AND research_run_spec_id = NEW.research_run_spec_id;
    IF NOT FOUND
       OR manifest_status <> NEW.status
       OR manifest_artifact_id IS DISTINCT FROM NEW.result_artifact_id THEN
        RAISE EXCEPTION 'governed Phase 1A outcome attempt requires one matching replay manifest';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER research_run_attempts_require_outcome_manifest
AFTER INSERT OR UPDATE
ON systematic_fx.research_run_attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION systematic_fx.require_phase1a_outcome_attempt_manifest();

CREATE FUNCTION systematic_fx.reject_phase1a_outcome_artifact_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM systematic_fx.phase1a_outcome_replay_checkpoints AS checkpoint
        WHERE checkpoint.checkpoint_artifact_id = OLD.artifact_id
    ) OR EXISTS (
        SELECT 1
        FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
        WHERE manifest.result_artifact_id = OLD.artifact_id
    ) THEN
        RAISE EXCEPTION 'Phase 1A outcome replay artifacts are immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER artifacts_protect_phase1a_outcome_replay
BEFORE UPDATE OR DELETE
ON systematic_fx.artifacts
FOR EACH ROW EXECUTE FUNCTION systematic_fx.reject_phase1a_outcome_artifact_mutation();

CREATE TRIGGER phase1a_outcome_manifests_publication_refresh
AFTER INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_outcome_replay_manifests
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();

CREATE TRIGGER phase1a_outcome_cells_publication_refresh
AFTER INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_outcome_cell_summaries
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();

COMMENT ON TABLE systematic_fx.phase1a_outcome_replay_manifests IS
    'Append-preserved Phase 1A p5 outcome replay attempts, each owned by one canonical RunSpec.';
COMMENT ON TABLE systematic_fx.phase1a_outcome_replay_checkpoints IS
    'Append-only SOURCE_DATE_COMPLETE artifacts forming one monotonic replay hash chain.';
COMMENT ON TABLE systematic_fx.phase1a_outcome_cell_summaries IS
    'Normalized complete 3-scenario by 2-direction by 484-cell Phase 1A p5 outcome surface.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (13, 'phase1a_outcome_replay', :'migration_checksum');

COMMIT;
