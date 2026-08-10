BEGIN;

CREATE OR REPLACE FUNCTION systematic_fx.protect_phase1a_ordered_outcome_manifest()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    spec record;
    spec_parameters jsonb;
    attempt record;
    result_artifact record;
    observed_summary_count integer;
    expected_config_id text;
    expected_occurrences integer;
    expected_detail_count integer;
    expected_planned_count integer;
    expected_long_count integer;
    expected_short_count integer;
    predecessor_audit record;
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
           OR NEW.source_artifact_manifest_sha256 IS DISTINCT FROM
                OLD.source_artifact_manifest_sha256
           OR NEW.source_slice_count IS DISTINCT FROM OLD.source_slice_count
           OR NEW.source_occurrence_count IS DISTINCT FROM OLD.source_occurrence_count
           OR NEW.scenario_count IS DISTINCT FROM OLD.scenario_count
           OR NEW.direction_count IS DISTINCT FROM OLD.direction_count
           OR NEW.barrier_axis_size IS DISTINCT FROM OLD.barrier_axis_size
           OR NEW.cell_count_per_surface IS DISTINCT FROM OLD.cell_count_per_surface
           OR NEW.expected_summary_count IS DISTINCT FROM OLD.expected_summary_count
           OR NEW.expected_detail_record_count IS DISTINCT FROM
                OLD.expected_detail_record_count
           OR NEW.planned_source_date_count IS DISTINCT FROM
                OLD.planned_source_date_count
           OR NEW.final_source_date IS DISTINCT FROM OLD.final_source_date
           OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
            RAISE EXCEPTION 'Phase 1A outcome replay manifest identity is immutable';
        END IF;
        IF NOT ((OLD.status = 'QUEUED' AND NEW.status IN ('RUNNING', 'FAILED'))
                OR (OLD.status = 'RUNNING' AND NEW.status IN ('SUCCEEDED', 'FAILED'))) THEN
            RAISE EXCEPTION 'invalid Phase 1A outcome replay status transition';
        END IF;
    END IF;

    IF NEW.pattern_key = 'p5_01_range_expansion_flow_continuation' THEN
        expected_config_id := 'phase1a_p5_outcome_replay_v1';
        expected_occurrences := 1111;
        expected_detail_count := 1613172;
        expected_planned_count := 485;
        expected_long_count := 529;
        expected_short_count := 582;
    ELSIF NEW.pattern_key = 'p1_05_unconfirmed_move_reversal' THEN
        expected_config_id := 'phase1a_p1_05_outcome_replay_v1';
        expected_occurrences := 943;
        expected_detail_count := 1369236;
        expected_planned_count := 478;
        expected_long_count := 446;
        expected_short_count := 497;
    ELSE
        RAISE EXCEPTION 'unknown ordered Phase 1A outcome candidate';
    END IF;

    SELECT campaign_source.campaign_key, run_spec_source.run_kind,
           run_spec_source.engine_version, run_spec_source.direction,
           run_spec_source.experiment_id,
           run_spec_source.canonical_spec -> 'parameters' AS parameters
    INTO STRICT spec
    FROM systematic_fx.research_run_specs AS run_spec_source
    JOIN systematic_fx.campaigns AS campaign_source
      ON campaign_source.campaign_id = run_spec_source.campaign_id
    WHERE run_spec_source.research_run_spec_id = NEW.research_run_spec_id
      AND run_spec_source.campaign_id = NEW.campaign_id
      AND run_spec_source.run_fingerprint = NEW.run_fingerprint;
    spec_parameters := spec.parameters;
    IF spec.campaign_key IS DISTINCT FROM 'phase1a_conservative_screening_v1'
       OR spec.run_kind IS DISTINCT FROM 'OUTCOME_BUILD'
       OR spec.engine_version IS DISTINCT FROM 'phase1a_shared_outcome_replay_v1'
       OR spec.direction IS DISTINCT FROM 'BOTH'
       OR spec.experiment_id IS NOT NULL
       OR spec_parameters #>> '{query_id}' IS DISTINCT FROM NEW.pattern_key
       OR spec_parameters #>> '{outcome_config_id}' IS DISTINCT FROM expected_config_id
       OR spec_parameters #>> '{source_artifact_manifest_sha256}' IS DISTINCT FROM
            NEW.source_artifact_manifest_sha256
       OR spec_parameters #>> '{source_slice_count}' IS DISTINCT FROM '99'
       OR spec_parameters #>> '{source_occurrence_count}' IS DISTINCT FROM
            expected_occurrences::text
       OR spec_parameters #>> '{cell_count_per_surface}' IS DISTINCT FROM '484'
       OR spec_parameters #>> '{expected_summary_count}' IS DISTINCT FROM '2904'
       OR spec_parameters #>> '{expected_detail_record_count}' IS DISTINCT FROM
            expected_detail_count::text
       OR spec_parameters #>> '{planned_source_date_count}' IS DISTINCT FROM
            expected_planned_count::text
       OR spec_parameters #>> '{final_source_date}' IS DISTINCT FROM '2023-08-31'
       OR spec_parameters #> '{expected_direction_signal_counts}' IS DISTINCT FROM
            jsonb_build_object('LONG', expected_long_count,
                               'SHORT', expected_short_count)
       OR spec_parameters #> '{scenario_ids}' IS DISTINCT FROM
            '["BASELINE","MODERATE_COMBINED","SEVERE_DIAGNOSTIC"]'::jsonb
       OR spec_parameters #> '{direction_ids}' IS DISTINCT FROM '["LONG","SHORT"]'::jsonb
       OR spec_parameters #> '{take_profit_ticks}' IS DISTINCT FROM
            '[24,32,40,48,56,64,72,80,88,96,104,112,120,128,136,144,152,160,168,176,184,192]'::jsonb
       OR spec_parameters #> '{stop_loss_ticks}' IS DISTINCT FROM
            '[24,32,40,48,56,64,72,80,88,96,104,112,120,128,136,144,152,160,168,176,184,192]'::jsonb THEN
        RAISE EXCEPTION 'ordered Phase 1A outcome RunSpec parameter drift';
    END IF;

    IF NEW.pattern_key = 'p1_05_unconfirmed_move_reversal' THEN
        SELECT audit_source.*,
               audit_artifact_source.sha256 AS audit_artifact_sha256
        INTO STRICT predecessor_audit
        FROM systematic_fx.phase1a_outcome_replay_equivalence_audits AS audit_source
        JOIN systematic_fx.artifacts AS audit_artifact_source
          ON audit_artifact_source.artifact_id = audit_source.audit_artifact_id
        WHERE audit_source.outcome_equivalence_audit_id =
              (spec_parameters #>> '{predecessor_equivalence_audit_id}')::bigint
          AND audit_source.passed;
        IF spec_parameters #>> '{predecessor_equivalence_audit_artifact_sha256}' IS DISTINCT FROM
                predecessor_audit.audit_artifact_sha256
           OR spec_parameters #>> '{predecessor_outcome_replay_manifest_id}' IS DISTINCT FROM
                predecessor_audit.predecessor_outcome_replay_manifest_id::text
           OR spec_parameters #>> '{predecessor_run_fingerprint}' IS DISTINCT FROM
                predecessor_audit.predecessor_run_fingerprint
           OR spec_parameters #>> '{predecessor_result_artifact_sha256}' IS DISTINCT FROM
                predecessor_audit.predecessor_result_artifact_sha256
           OR spec_parameters #>> '{predecessor_input_lineage_sha256}' IS DISTINCT FROM
                predecessor_audit.input_lineage_sha256
           OR spec_parameters #>> '{predecessor_cell_summaries_sha256}' IS DISTINCT FROM
                predecessor_audit.cell_summaries_sha256
           OR spec_parameters #>> '{predecessor_detail_shard_manifest_sha256}' IS DISTINCT FROM
                predecessor_audit.detail_shard_manifest_sha256
           OR spec_parameters #>> '{predecessor_final_checkpoint_sha256}' IS DISTINCT FROM
                predecessor_audit.final_checkpoint_sha256 THEN
            RAISE EXCEPTION 'p1_05 predecessor equivalence lineage drift';
        END IF;
    END IF;

    SELECT attempt_source.status, attempt_source.result_artifact_id,
           attempt_source.started_at, attempt_source.finished_at,
           attempt_source.error_message, attempt_source.result_summary
    INTO STRICT attempt
    FROM systematic_fx.research_run_attempts AS attempt_source
    WHERE attempt_source.research_run_attempt_id = NEW.research_run_attempt_id
      AND attempt_source.research_run_spec_id = NEW.research_run_spec_id;
    IF attempt.status IS DISTINCT FROM NEW.status
       OR attempt.result_artifact_id IS DISTINCT FROM NEW.result_artifact_id
       OR attempt.started_at IS DISTINCT FROM NEW.started_at
       OR attempt.finished_at IS DISTINCT FROM NEW.finished_at
       OR attempt.error_message IS DISTINCT FROM NEW.error_message THEN
        RAISE EXCEPTION 'outcome replay manifest and run attempt state differ';
    END IF;

    IF NEW.status = 'SUCCEEDED' THEN
        SELECT result_artifact_source.artifact_type,
               result_artifact_source.uri,
               result_artifact_source.sha256,
               result_artifact_source.byte_size,
               result_artifact_source.metadata
        INTO STRICT result_artifact
        FROM systematic_fx.artifacts AS result_artifact_source
        WHERE result_artifact_source.artifact_id = NEW.result_artifact_id;
        IF result_artifact.artifact_type IS DISTINCT FROM
                'PHASE1A_OUTCOME_REPLAY_RESULT'
           OR position('/data/derived/' IN result_artifact.uri) = 0
           OR result_artifact.sha256 IS DISTINCT FROM NEW.result_artifact_sha256
           OR result_artifact.byte_size IS DISTINCT FROM NEW.result_artifact_byte_size
           OR result_artifact.metadata #>> '{campaign_key}' IS DISTINCT FROM
                'phase1a_conservative_screening_v1'
           OR result_artifact.metadata #>> '{query_id}' IS DISTINCT FROM NEW.pattern_key
           OR result_artifact.metadata #>> '{outcome_config_id}' IS DISTINCT FROM
                expected_config_id
           OR result_artifact.metadata #>> '{run_fingerprint}' IS DISTINCT FROM
                NEW.run_fingerprint
           OR result_artifact.metadata #>> '{source_artifact_manifest_sha256}' IS DISTINCT FROM
                NEW.source_artifact_manifest_sha256
           OR result_artifact.metadata #>> '{cell_summaries_sha256}' IS DISTINCT FROM
                NEW.cell_summaries_sha256
           OR result_artifact.metadata #>> '{summary_row_count}' IS DISTINCT FROM
                '2904' THEN
            RAISE EXCEPTION 'ordered Phase 1A outcome result artifact lineage drift';
        END IF;
        IF NEW.pattern_key = 'p1_05_unconfirmed_move_reversal'
           AND (result_artifact.metadata #>> '{predecessor_equivalence_audit_id}'
                    IS DISTINCT FROM
                    predecessor_audit.outcome_equivalence_audit_id::text
                OR result_artifact.metadata #>>
                    '{predecessor_equivalence_audit_artifact_sha256}' IS DISTINCT FROM
                    predecessor_audit.audit_artifact_sha256
                OR result_artifact.metadata #>> '{predecessor_outcome_replay_manifest_id}'
                    IS DISTINCT FROM
                    predecessor_audit.predecessor_outcome_replay_manifest_id::text
                OR result_artifact.metadata #>> '{predecessor_run_fingerprint}'
                    IS DISTINCT FROM predecessor_audit.predecessor_run_fingerprint
                OR result_artifact.metadata #>> '{predecessor_result_artifact_sha256}'
                    IS DISTINCT FROM
                    predecessor_audit.predecessor_result_artifact_sha256
                OR result_artifact.metadata #>> '{predecessor_input_lineage_sha256}'
                    IS DISTINCT FROM predecessor_audit.input_lineage_sha256
                OR result_artifact.metadata #>> '{predecessor_cell_summaries_sha256}'
                    IS DISTINCT FROM predecessor_audit.cell_summaries_sha256
                OR result_artifact.metadata #>>
                    '{predecessor_detail_shard_manifest_sha256}' IS DISTINCT FROM
                    predecessor_audit.detail_shard_manifest_sha256
                OR result_artifact.metadata #>> '{predecessor_final_checkpoint_sha256}'
                    IS DISTINCT FROM predecessor_audit.final_checkpoint_sha256) THEN
            RAISE EXCEPTION 'p1_05 result predecessor audit lineage drift';
        END IF;
        SELECT count(*)::integer INTO observed_summary_count
        FROM systematic_fx.phase1a_outcome_cell_summaries AS summary_source
        WHERE summary_source.outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
          AND summary_source.run_fingerprint = NEW.run_fingerprint;
        IF observed_summary_count IS DISTINCT FROM 2904 THEN
            RAISE EXCEPTION 'Phase 1A outcome replay requires all 2904 cell summaries';
        END IF;
        IF attempt.result_summary #>> '{artifact_sha256}' IS DISTINCT FROM
                NEW.result_artifact_sha256
           OR attempt.result_summary #>> '{cell_summaries_sha256}' IS DISTINCT FROM
                NEW.cell_summaries_sha256
           OR attempt.result_summary #>> '{summary_row_count}' IS DISTINCT FROM '2904'
           OR attempt.result_summary #>> '{source_artifact_manifest_sha256}' IS DISTINCT FROM
                NEW.source_artifact_manifest_sha256 THEN
            RAISE EXCEPTION 'Phase 1A outcome replay attempt summary drift';
        END IF;
        IF NEW.pattern_key = 'p1_05_unconfirmed_move_reversal'
           AND (attempt.result_summary #>> '{predecessor_equivalence_audit_id}'
                    IS DISTINCT FROM
                    predecessor_audit.outcome_equivalence_audit_id::text
                OR attempt.result_summary #>>
                    '{predecessor_equivalence_audit_artifact_sha256}'
                    IS DISTINCT FROM predecessor_audit.audit_artifact_sha256
                OR attempt.result_summary #>>
                    '{predecessor_outcome_replay_manifest_id}' IS DISTINCT FROM
                    predecessor_audit.predecessor_outcome_replay_manifest_id::text
                OR attempt.result_summary #>> '{predecessor_run_fingerprint}'
                    IS DISTINCT FROM predecessor_audit.predecessor_run_fingerprint
                OR attempt.result_summary #>>
                    '{predecessor_result_artifact_sha256}' IS DISTINCT FROM
                    predecessor_audit.predecessor_result_artifact_sha256
                OR attempt.result_summary #>>
                    '{predecessor_input_lineage_sha256}' IS DISTINCT FROM
                    predecessor_audit.input_lineage_sha256
                OR attempt.result_summary #>>
                    '{predecessor_cell_summaries_sha256}' IS DISTINCT FROM
                    predecessor_audit.cell_summaries_sha256
                OR attempt.result_summary #>>
                    '{predecessor_detail_shard_manifest_sha256}' IS DISTINCT FROM
                    predecessor_audit.detail_shard_manifest_sha256
                OR attempt.result_summary #>>
                    '{predecessor_final_checkpoint_sha256}' IS DISTINCT FROM
                    predecessor_audit.final_checkpoint_sha256) THEN
            RAISE EXCEPTION 'p1_05 attempt predecessor audit lineage drift';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (21, 'phase1a_outcome_manifest_record_alias_hardening', :'migration_checksum');

COMMIT;
