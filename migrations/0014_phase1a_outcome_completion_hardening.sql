BEGIN;

ALTER TABLE systematic_fx.phase1a_outcome_replay_manifests
    ADD COLUMN expected_detail_record_count integer NOT NULL DEFAULT 1613172,
    ADD COLUMN planned_source_date_count smallint NOT NULL DEFAULT 485,
    ADD COLUMN final_source_date date NOT NULL DEFAULT DATE '2023-08-31',
    ADD CONSTRAINT phase1a_outcome_manifests_frozen_completion_plan
        CHECK (expected_detail_record_count = 1613172
               AND planned_source_date_count = 485
               AND final_source_date = DATE '2023-08-31');

ALTER TABLE systematic_fx.phase1a_outcome_cell_summaries
    ADD CONSTRAINT phase1a_outcome_cells_frozen_signal_count
        CHECK ((direction = 'LONG' AND signal_count = 529)
               OR (direction = 'SHORT' AND signal_count = 582))
        NOT VALID,
    ADD CONSTRAINT phase1a_outcome_cells_frozen_cost_accounting
        CHECK (variable_cost_ticks = entry_fill_count
                    * CASE scenario_id
                        WHEN 'BASELINE' THEN 4
                        WHEN 'MODERATE_COMBINED' THEN 5
                        WHEN 'SEVERE_DIAGNOSTIC' THEN 6
                      END
               AND allocated_fixed_cost_ticks = entry_fill_count
                    * CASE scenario_id
                        WHEN 'BASELINE' THEN 4
                        WHEN 'MODERATE_COMBINED' THEN 5
                        WHEN 'SEVERE_DIAGNOSTIC' THEN 6
                      END)
        NOT VALID;

CREATE FUNCTION systematic_fx.harden_phase1a_outcome_completion()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    spec_parameters jsonb;
    attempt_summary jsonb;
    artifact_metadata_value jsonb;
    observed_checkpoint_count integer;
    final_checkpoint_sequence bigint;
    final_completed_source_date_count integer;
    final_checkpoint_source_date date;
    final_checkpoint_sha256 text;
    final_checkpoint_progress jsonb;
BEGIN
    IF TG_OP = 'UPDATE'
       AND (NEW.expected_detail_record_count
                IS DISTINCT FROM OLD.expected_detail_record_count
            OR NEW.planned_source_date_count
                IS DISTINCT FROM OLD.planned_source_date_count
            OR NEW.final_source_date IS DISTINCT FROM OLD.final_source_date) THEN
        RAISE EXCEPTION 'Phase 1A outcome replay completion plan is immutable';
    END IF;

    SELECT run_spec.canonical_spec -> 'parameters'
    INTO STRICT spec_parameters
    FROM systematic_fx.research_run_specs AS run_spec
    WHERE run_spec.research_run_spec_id = NEW.research_run_spec_id
      AND run_spec.campaign_id = NEW.campaign_id
      AND run_spec.run_fingerprint = NEW.run_fingerprint;

    IF spec_parameters #>> '{query_id}' IS DISTINCT FROM
            'p5_01_range_expansion_flow_continuation'
       OR spec_parameters #>> '{outcome_config_id}' IS DISTINCT FROM
            'phase1a_p5_outcome_replay_v1'
       OR spec_parameters #>> '{source_artifact_manifest_sha256}'
            IS DISTINCT FROM NEW.source_artifact_manifest_sha256
       OR spec_parameters #>> '{source_slice_count}' IS DISTINCT FROM '99'
       OR spec_parameters #>> '{source_occurrence_count}' IS DISTINCT FROM '1111'
       OR spec_parameters #>> '{cell_count_per_surface}' IS DISTINCT FROM '484'
       OR spec_parameters #>> '{expected_summary_count}' IS DISTINCT FROM '2904'
       OR spec_parameters #>> '{expected_detail_record_count}' IS DISTINCT FROM '1613172'
       OR spec_parameters #>> '{planned_source_date_count}' IS DISTINCT FROM '485'
       OR spec_parameters #>> '{final_source_date}' IS DISTINCT FROM '2023-08-31'
       OR spec_parameters #>> '{expected_completed_source_date_count}'
            IS DISTINCT FROM '485'
       OR spec_parameters #>> '{expected_last_completed_source_date}'
            IS DISTINCT FROM '2023-08-31'
       OR spec_parameters #> '{expected_direction_signal_counts}' IS DISTINCT FROM
            '{"LONG":529,"SHORT":582}'::jsonb
       OR spec_parameters #> '{scenario_cost_ticks_per_fill}' IS DISTINCT FROM
            '{"BASELINE":{"allocated_fixed":4,"variable":4},"MODERATE_COMBINED":{"allocated_fixed":5,"variable":5},"SEVERE_DIAGNOSTIC":{"allocated_fixed":6,"variable":6}}'::jsonb
       OR spec_parameters #> '{scenario_ids}' IS DISTINCT FROM
            '["BASELINE", "MODERATE_COMBINED", "SEVERE_DIAGNOSTIC"]'::jsonb
       OR spec_parameters #> '{direction_ids}' IS DISTINCT FROM
            '["LONG", "SHORT"]'::jsonb
       OR spec_parameters #> '{take_profit_ticks}' IS DISTINCT FROM
            '[24,32,40,48,56,64,72,80,88,96,104,112,120,128,136,144,152,160,168,176,184,192]'::jsonb
       OR spec_parameters #> '{stop_loss_ticks}' IS DISTINCT FROM
            '[24,32,40,48,56,64,72,80,88,96,104,112,120,128,136,144,152,160,168,176,184,192]'::jsonb THEN
        RAISE EXCEPTION 'Phase 1A p5 outcome RunSpec completion parameter drift';
    END IF;

    IF NEW.status <> 'SUCCEEDED' THEN
        RETURN NEW;
    END IF;

    SELECT artifact.metadata
    INTO STRICT artifact_metadata_value
    FROM systematic_fx.artifacts AS artifact
    WHERE artifact.artifact_id = NEW.result_artifact_id;

    IF artifact_metadata_value #>> '{campaign_key}' IS DISTINCT FROM
            'phase1a_conservative_screening_v1'
       OR artifact_metadata_value #>> '{query_id}' IS DISTINCT FROM
            'p5_01_range_expansion_flow_continuation'
       OR artifact_metadata_value #>> '{outcome_config_id}' IS DISTINCT FROM
            'phase1a_p5_outcome_replay_v1'
       OR artifact_metadata_value #>> '{run_fingerprint}'
            IS DISTINCT FROM NEW.run_fingerprint
       OR artifact_metadata_value #>> '{source_artifact_manifest_sha256}'
            IS DISTINCT FROM NEW.source_artifact_manifest_sha256
       OR artifact_metadata_value #>> '{cell_summaries_sha256}'
            IS DISTINCT FROM NEW.cell_summaries_sha256
       OR artifact_metadata_value #>> '{summary_row_count}' IS DISTINCT FROM '2904'
       OR artifact_metadata_value #>> '{detail_record_count}' IS DISTINCT FROM '1613172'
       OR artifact_metadata_value #>> '{detail_shard_count}' IS DISTINCT FROM '485'
       OR artifact_metadata_value #>> '{planned_source_date_count}' IS DISTINCT FROM '485'
       OR COALESCE(artifact_metadata_value #>> '{cache_manifest_sha256}', '')
            !~ '^[0-9a-f]{64}$'
       OR COALESCE(artifact_metadata_value #>> '{detail_shard_manifest_sha256}', '')
            !~ '^[0-9a-f]{64}$'
       OR COALESCE(artifact_metadata_value #>> '{final_checkpoint_sha256}', '')
            !~ '^[0-9a-f]{64}$'
       OR COALESCE(artifact_metadata_value #>> '{input_lineage_sha256}', '')
            !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'Phase 1A outcome replay result completion lineage drift';
    END IF;

    SELECT count(*)::integer
    INTO observed_checkpoint_count
    FROM systematic_fx.phase1a_outcome_replay_checkpoints
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
      AND run_fingerprint = NEW.run_fingerprint;
    IF observed_checkpoint_count <> NEW.planned_source_date_count THEN
        RAISE EXCEPTION 'Phase 1A outcome replay requires all 485 source-date checkpoints';
    END IF;

    SELECT checkpoint_sequence, completed_source_date_count,
           last_completed_source_date, checkpoint_artifact_sha256,
           progress_metadata
    INTO STRICT final_checkpoint_sequence,
                final_completed_source_date_count,
                final_checkpoint_source_date,
                final_checkpoint_sha256,
                final_checkpoint_progress
    FROM systematic_fx.phase1a_outcome_replay_checkpoints
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
      AND run_fingerprint = NEW.run_fingerprint
    ORDER BY checkpoint_sequence DESC
    LIMIT 1;

    IF final_checkpoint_sequence IS DISTINCT FROM NEW.planned_source_date_count
       OR final_completed_source_date_count IS DISTINCT FROM NEW.planned_source_date_count
       OR final_checkpoint_source_date IS DISTINCT FROM NEW.final_source_date
       OR final_checkpoint_sha256 IS DISTINCT FROM
            artifact_metadata_value #>> '{final_checkpoint_sha256}'
       OR final_checkpoint_progress #>> '{artifact_schema}' IS DISTINCT FROM
            'systematic_fx.phase1a_outcome_progress.v1'
       OR final_checkpoint_progress #>> '{replay_finished}' IS DISTINCT FROM 'true'
       OR final_checkpoint_progress #>> '{detail_record_count}' IS DISTINCT FROM '1613172'
       OR final_checkpoint_progress #>> '{detail_shard_count}' IS DISTINCT FROM '485'
       OR final_checkpoint_progress #>> '{cache_manifest_sha256}' IS DISTINCT FROM
            artifact_metadata_value #>> '{cache_manifest_sha256}'
       OR final_checkpoint_progress #>> '{detail_shard_manifest_sha256}' IS DISTINCT FROM
            artifact_metadata_value #>> '{detail_shard_manifest_sha256}'
       OR final_checkpoint_progress #>> '{input_lineage_sha256}' IS DISTINCT FROM
            artifact_metadata_value #>> '{input_lineage_sha256}' THEN
        RAISE EXCEPTION 'Phase 1A outcome replay final checkpoint is incomplete or unbound';
    END IF;

    SELECT attempt.result_summary
    INTO STRICT attempt_summary
    FROM systematic_fx.research_run_attempts AS attempt
    WHERE attempt.research_run_attempt_id = NEW.research_run_attempt_id
      AND attempt.research_run_spec_id = NEW.research_run_spec_id;

    IF attempt_summary #>> '{artifact_sha256}'
            IS DISTINCT FROM NEW.result_artifact_sha256
       OR attempt_summary #>> '{cell_summaries_sha256}'
            IS DISTINCT FROM NEW.cell_summaries_sha256
       OR attempt_summary #>> '{summary_row_count}' IS DISTINCT FROM '2904'
       OR attempt_summary #>> '{detail_record_count}' IS DISTINCT FROM '1613172'
       OR attempt_summary #>> '{detail_shard_count}' IS DISTINCT FROM '485'
       OR attempt_summary #>> '{planned_source_date_count}' IS DISTINCT FROM '485'
       OR attempt_summary #>> '{cache_manifest_sha256}' IS DISTINCT FROM
            artifact_metadata_value #>> '{cache_manifest_sha256}'
       OR attempt_summary #>> '{detail_shard_manifest_sha256}' IS DISTINCT FROM
            artifact_metadata_value #>> '{detail_shard_manifest_sha256}'
       OR attempt_summary #>> '{final_checkpoint_sha256}' IS DISTINCT FROM
            artifact_metadata_value #>> '{final_checkpoint_sha256}'
       OR attempt_summary #>> '{input_lineage_sha256}' IS DISTINCT FROM
            artifact_metadata_value #>> '{input_lineage_sha256}'
       OR attempt_summary #>> '{source_artifact_manifest_sha256}'
            IS DISTINCT FROM NEW.source_artifact_manifest_sha256 THEN
        RAISE EXCEPTION 'Phase 1A outcome replay attempt completion summary drift';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER phase1a_outcome_manifest_completion_hardening
BEFORE INSERT OR UPDATE
ON systematic_fx.phase1a_outcome_replay_manifests
FOR EACH ROW EXECUTE FUNCTION systematic_fx.harden_phase1a_outcome_completion();

COMMENT ON FUNCTION systematic_fx.harden_phase1a_outcome_completion() IS
    'Fail-closed Phase 1A p5 completion gate for the frozen 485-date replay, 1,613,172 details, and final checkpoint lineage.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (14, 'phase1a_outcome_completion_hardening', :'migration_checksum');

COMMIT;
