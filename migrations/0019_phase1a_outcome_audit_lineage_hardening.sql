BEGIN;

-- PostgreSQL three-valued logic makes a missing JSON path comparison NULL
-- and therefore skips a PL/pgSQL IF branch. Reinstall every ordered-candidate
-- audit/completion guard with NULL-safe comparisons.
CREATE OR REPLACE FUNCTION systematic_fx.protect_phase1a_outcome_equivalence_audit()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    subject record;
    subject_metadata jsonb;
    campaign_key_value text;
    validation_spec record;
    validation_parameters jsonb;
    validation_attempt record;
    audit_artifact record;
BEGIN
    IF TG_OP IS DISTINCT FROM 'INSERT' THEN
        RAISE EXCEPTION 'Phase 1A outcome equivalence audits are append-only';
    END IF;

    SELECT manifest.*, artifact.metadata AS result_metadata
    INTO STRICT subject
    FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
    JOIN systematic_fx.artifacts AS artifact
      ON artifact.artifact_id = manifest.result_artifact_id
    WHERE manifest.outcome_replay_manifest_id =
          NEW.predecessor_outcome_replay_manifest_id;
    subject_metadata := subject.result_metadata;
    IF subject.pattern_key IS DISTINCT FROM 'p5_01_range_expansion_flow_continuation'
       OR subject.status IS DISTINCT FROM 'SUCCEEDED'
       OR subject.campaign_id IS DISTINCT FROM NEW.campaign_id
       OR subject.run_fingerprint IS DISTINCT FROM NEW.predecessor_run_fingerprint
       OR subject.result_artifact_sha256 IS DISTINCT FROM
            NEW.predecessor_result_artifact_sha256
       OR subject.cell_summaries_sha256 IS DISTINCT FROM NEW.cell_summaries_sha256
       OR subject_metadata #>> '{cache_manifest_sha256}' IS DISTINCT FROM
            NEW.cache_manifest_sha256
       OR subject_metadata #>> '{detail_shard_manifest_sha256}' IS DISTINCT FROM
            NEW.detail_shard_manifest_sha256
       OR subject_metadata #>> '{input_lineage_sha256}' IS DISTINCT FROM
            NEW.input_lineage_sha256
       OR subject_metadata #>> '{final_checkpoint_sha256}' IS DISTINCT FROM
            NEW.final_checkpoint_sha256 THEN
        RAISE EXCEPTION 'p5 equivalence audit subject lineage drift';
    END IF;

    SELECT campaign.campaign_key, run_spec.run_kind, run_spec.engine_version,
           run_spec.direction, run_spec.experiment_id,
           run_spec.canonical_spec -> 'parameters' AS parameters
    INTO STRICT validation_spec
    FROM systematic_fx.research_run_specs AS run_spec
    JOIN systematic_fx.campaigns AS campaign
      ON campaign.campaign_id = run_spec.campaign_id
    WHERE run_spec.research_run_spec_id = NEW.validation_research_run_spec_id
      AND run_spec.campaign_id = NEW.campaign_id
      AND run_spec.run_fingerprint = NEW.validation_run_fingerprint;
    validation_parameters := validation_spec.parameters;
    campaign_key_value := validation_spec.campaign_key;
    IF campaign_key_value IS DISTINCT FROM 'phase1a_conservative_screening_v1'
       OR validation_spec.run_kind IS DISTINCT FROM 'VALIDATION'
       OR validation_spec.engine_version IS DISTINCT FROM
            'phase1a_outcome_equivalence_audit_v1'
       OR validation_spec.direction IS DISTINCT FROM 'BOTH'
       OR validation_spec.experiment_id IS NOT NULL
       OR validation_parameters #>> '{audit_kind}' IS DISTINCT FROM
            'UNINTERRUPTED_VS_RESUMED_BYTE_EQUIVALENCE'
       OR validation_parameters #>> '{query_id}' IS DISTINCT FROM
            'p5_01_range_expansion_flow_continuation'
       OR validation_parameters #>> '{predecessor_outcome_replay_manifest_id}' IS DISTINCT FROM
            NEW.predecessor_outcome_replay_manifest_id::text
       OR validation_parameters #>> '{predecessor_run_fingerprint}' IS DISTINCT FROM
            NEW.predecessor_run_fingerprint
       OR validation_parameters #>> '{predecessor_result_artifact_sha256}' IS DISTINCT FROM
            NEW.predecessor_result_artifact_sha256
       OR validation_parameters #>> '{cache_manifest_sha256}' IS DISTINCT FROM
            NEW.cache_manifest_sha256
       OR validation_parameters #>> '{cell_summaries_sha256}' IS DISTINCT FROM
            NEW.cell_summaries_sha256
       OR validation_parameters #>> '{detail_shard_manifest_sha256}' IS DISTINCT FROM
            NEW.detail_shard_manifest_sha256
       OR validation_parameters #>> '{input_lineage_sha256}' IS DISTINCT FROM
            NEW.input_lineage_sha256
       OR validation_parameters #>> '{final_checkpoint_sha256}' IS DISTINCT FROM
            NEW.final_checkpoint_sha256
       OR validation_parameters #>> '{checkpoint_chain_sha256}' IS DISTINCT FROM
            NEW.checkpoint_chain_sha256
       OR validation_parameters #>> '{checkpoint_count}' IS DISTINCT FROM
            NEW.checkpoint_count::text THEN
        RAISE EXCEPTION 'p5 equivalence audit validation RunSpec drift';
    END IF;

    SELECT status, result_artifact_id, result_summary
    INTO STRICT validation_attempt
    FROM systematic_fx.research_run_attempts
    WHERE research_run_attempt_id = NEW.validation_research_run_attempt_id
      AND research_run_spec_id = NEW.validation_research_run_spec_id;
    IF validation_attempt.status IS DISTINCT FROM 'SUCCEEDED'
       OR validation_attempt.result_artifact_id IS DISTINCT FROM NEW.audit_artifact_id
       OR validation_attempt.result_summary #>> '{audit_artifact_sha256}' IS DISTINCT FROM
            NEW.audit_artifact_sha256
       OR validation_attempt.result_summary #>> '{checkpoint_chain_sha256}' IS DISTINCT FROM
            NEW.checkpoint_chain_sha256
       OR validation_attempt.result_summary #>> '{passed}' IS DISTINCT FROM 'true' THEN
        RAISE EXCEPTION 'p5 equivalence audit validation attempt drift';
    END IF;

    SELECT artifact_type, uri, sha256, byte_size, media_type, metadata
    INTO STRICT audit_artifact
    FROM systematic_fx.artifacts
    WHERE artifact_id = NEW.audit_artifact_id;
    IF audit_artifact.artifact_type IS DISTINCT FROM
            'PHASE1A_OUTCOME_REPLAY_EQUIVALENCE_AUDIT'
       OR position('/data/derived/' IN audit_artifact.uri) = 0
       OR audit_artifact.sha256 IS DISTINCT FROM NEW.audit_artifact_sha256
       OR audit_artifact.byte_size IS DISTINCT FROM NEW.audit_artifact_byte_size
       OR audit_artifact.media_type IS DISTINCT FROM 'application/json'
       OR audit_artifact.metadata #>> '{campaign_key}' IS DISTINCT FROM
            'phase1a_conservative_screening_v1'
       OR audit_artifact.metadata #>> '{audit_kind}' IS DISTINCT FROM
            'UNINTERRUPTED_VS_RESUMED_BYTE_EQUIVALENCE'
       OR audit_artifact.metadata #>> '{predecessor_outcome_replay_manifest_id}' IS DISTINCT FROM
            NEW.predecessor_outcome_replay_manifest_id::text
       OR audit_artifact.metadata #>> '{predecessor_result_artifact_sha256}' IS DISTINCT FROM
            NEW.predecessor_result_artifact_sha256
       OR audit_artifact.metadata #>> '{checkpoint_chain_sha256}' IS DISTINCT FROM
            NEW.checkpoint_chain_sha256
       OR audit_artifact.metadata #>> '{validation_run_fingerprint}' IS DISTINCT FROM
            NEW.validation_run_fingerprint
       OR audit_artifact.metadata #>> '{passed}' IS DISTINCT FROM 'true' THEN
        RAISE EXCEPTION 'p5 equivalence audit artifact lineage drift';
    END IF;
    RETURN NEW;
END;
$$;
CREATE OR REPLACE FUNCTION systematic_fx.protect_phase1a_ordered_outcome_manifest()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    spec record;
    spec_parameters jsonb;
    attempt record;
    artifact record;
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

    SELECT campaign.campaign_key, run_spec.run_kind, run_spec.engine_version,
           run_spec.direction, run_spec.experiment_id,
           run_spec.canonical_spec -> 'parameters' AS parameters
    INTO STRICT spec
    FROM systematic_fx.research_run_specs AS run_spec
    JOIN systematic_fx.campaigns AS campaign
      ON campaign.campaign_id = run_spec.campaign_id
    WHERE run_spec.research_run_spec_id = NEW.research_run_spec_id
      AND run_spec.campaign_id = NEW.campaign_id
      AND run_spec.run_fingerprint = NEW.run_fingerprint;
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
        SELECT audit.*, artifact.sha256 AS audit_artifact_sha256
        INTO STRICT predecessor_audit
        FROM systematic_fx.phase1a_outcome_replay_equivalence_audits AS audit
        JOIN systematic_fx.artifacts AS artifact
          ON artifact.artifact_id = audit.audit_artifact_id
        WHERE audit.outcome_equivalence_audit_id =
              (spec_parameters #>> '{predecessor_equivalence_audit_id}')::bigint
          AND audit.passed;
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

    SELECT status, result_artifact_id, started_at, finished_at,
           error_message, result_summary
    INTO STRICT attempt
    FROM systematic_fx.research_run_attempts
    WHERE research_run_attempt_id = NEW.research_run_attempt_id
      AND research_run_spec_id = NEW.research_run_spec_id;
    IF attempt.status IS DISTINCT FROM NEW.status
       OR attempt.result_artifact_id IS DISTINCT FROM NEW.result_artifact_id
       OR attempt.started_at IS DISTINCT FROM NEW.started_at
       OR attempt.finished_at IS DISTINCT FROM NEW.finished_at
       OR attempt.error_message IS DISTINCT FROM NEW.error_message THEN
        RAISE EXCEPTION 'outcome replay manifest and run attempt state differ';
    END IF;

    IF NEW.status = 'SUCCEEDED' THEN
        SELECT artifact_type, uri, sha256, byte_size, metadata
        INTO STRICT artifact
        FROM systematic_fx.artifacts
        WHERE artifact_id = NEW.result_artifact_id;
        IF artifact.artifact_type IS DISTINCT FROM 'PHASE1A_OUTCOME_REPLAY_RESULT'
           OR position('/data/derived/' IN artifact.uri) = 0
           OR artifact.sha256 IS DISTINCT FROM NEW.result_artifact_sha256
           OR artifact.byte_size IS DISTINCT FROM NEW.result_artifact_byte_size
           OR artifact.metadata #>> '{campaign_key}' IS DISTINCT FROM
                'phase1a_conservative_screening_v1'
           OR artifact.metadata #>> '{query_id}' IS DISTINCT FROM NEW.pattern_key
           OR artifact.metadata #>> '{outcome_config_id}' IS DISTINCT FROM expected_config_id
           OR artifact.metadata #>> '{run_fingerprint}' IS DISTINCT FROM NEW.run_fingerprint
           OR artifact.metadata #>> '{source_artifact_manifest_sha256}' IS DISTINCT FROM
                NEW.source_artifact_manifest_sha256
           OR artifact.metadata #>> '{cell_summaries_sha256}' IS DISTINCT FROM
                NEW.cell_summaries_sha256
           OR artifact.metadata #>> '{summary_row_count}' IS DISTINCT FROM '2904' THEN
            RAISE EXCEPTION 'ordered Phase 1A outcome result artifact lineage drift';
        END IF;
        IF NEW.pattern_key = 'p1_05_unconfirmed_move_reversal'
           AND (artifact.metadata #>> '{predecessor_equivalence_audit_id}' IS DISTINCT FROM
                    predecessor_audit.outcome_equivalence_audit_id::text
                OR artifact.metadata #>>
                    '{predecessor_equivalence_audit_artifact_sha256}' IS DISTINCT FROM
                    predecessor_audit.audit_artifact_sha256
                OR artifact.metadata #>> '{predecessor_outcome_replay_manifest_id}'
                    IS DISTINCT FROM
                    predecessor_audit.predecessor_outcome_replay_manifest_id::text
                OR artifact.metadata #>> '{predecessor_run_fingerprint}'
                    IS DISTINCT FROM predecessor_audit.predecessor_run_fingerprint
                OR artifact.metadata #>> '{predecessor_result_artifact_sha256}'
                    IS DISTINCT FROM
                    predecessor_audit.predecessor_result_artifact_sha256
                OR artifact.metadata #>> '{predecessor_input_lineage_sha256}'
                    IS DISTINCT FROM predecessor_audit.input_lineage_sha256
                OR artifact.metadata #>> '{predecessor_cell_summaries_sha256}'
                    IS DISTINCT FROM predecessor_audit.cell_summaries_sha256
                OR artifact.metadata #>>
                    '{predecessor_detail_shard_manifest_sha256}' IS DISTINCT FROM
                    predecessor_audit.detail_shard_manifest_sha256
                OR artifact.metadata #>> '{predecessor_final_checkpoint_sha256}'
                    IS DISTINCT FROM predecessor_audit.final_checkpoint_sha256) THEN
            RAISE EXCEPTION 'p1_05 result predecessor audit lineage drift';
        END IF;
        SELECT count(*)::integer INTO observed_summary_count
        FROM systematic_fx.phase1a_outcome_cell_summaries
        WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
          AND run_fingerprint = NEW.run_fingerprint;
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
CREATE OR REPLACE FUNCTION systematic_fx.harden_phase1a_ordered_outcome_completion()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    parameters jsonb;
    attempt_summary jsonb;
    artifact_metadata jsonb;
    expected_config_id text;
    expected_occurrences integer;
    expected_detail_count integer;
    expected_planned_count integer;
    expected_long_count integer;
    expected_short_count integer;
    observed_checkpoint_count integer;
    final_checkpoint record;
BEGIN
    IF TG_OP = 'UPDATE'
       AND (NEW.expected_detail_record_count IS DISTINCT FROM
                OLD.expected_detail_record_count
            OR NEW.planned_source_date_count IS DISTINCT FROM
                OLD.planned_source_date_count
            OR NEW.final_source_date IS DISTINCT FROM OLD.final_source_date) THEN
        RAISE EXCEPTION 'Phase 1A outcome replay completion plan is immutable';
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

    SELECT canonical_spec -> 'parameters' INTO STRICT parameters
    FROM systematic_fx.research_run_specs
    WHERE research_run_spec_id = NEW.research_run_spec_id
      AND campaign_id = NEW.campaign_id
      AND run_fingerprint = NEW.run_fingerprint;
    IF parameters #>> '{query_id}' IS DISTINCT FROM NEW.pattern_key
       OR parameters #>> '{outcome_config_id}' IS DISTINCT FROM expected_config_id
       OR parameters #>> '{source_artifact_manifest_sha256}' IS DISTINCT FROM
            NEW.source_artifact_manifest_sha256
       OR parameters #>> '{source_slice_count}' IS DISTINCT FROM '99'
       OR parameters #>> '{source_occurrence_count}' IS DISTINCT FROM
            expected_occurrences::text
       OR parameters #>> '{cell_count_per_surface}' IS DISTINCT FROM '484'
       OR parameters #>> '{expected_summary_count}' IS DISTINCT FROM '2904'
       OR parameters #>> '{expected_detail_record_count}' IS DISTINCT FROM
            expected_detail_count::text
       OR parameters #>> '{planned_source_date_count}' IS DISTINCT FROM
            expected_planned_count::text
       OR parameters #>> '{expected_completed_source_date_count}' IS DISTINCT FROM
            expected_planned_count::text
       OR parameters #>> '{expected_last_completed_source_date}' IS DISTINCT FROM
            '2023-08-31'
       OR parameters #> '{expected_direction_signal_counts}' IS DISTINCT FROM
            jsonb_build_object('LONG', expected_long_count,
                               'SHORT', expected_short_count)
       OR parameters #> '{scenario_cost_ticks_per_fill}' IS DISTINCT FROM
            '{"BASELINE":{"allocated_fixed":4,"variable":4},"MODERATE_COMBINED":{"allocated_fixed":5,"variable":5},"SEVERE_DIAGNOSTIC":{"allocated_fixed":6,"variable":6}}'::jsonb THEN
        RAISE EXCEPTION 'ordered Phase 1A outcome completion parameter drift';
    END IF;
    IF NEW.pattern_key = 'p1_05_unconfirmed_move_reversal'
       AND (parameters #>> '{predecessor_equivalence_audit_id}' IS NULL
            OR parameters #>>
                '{predecessor_equivalence_audit_artifact_sha256}' IS NULL
            OR parameters #>> '{predecessor_outcome_replay_manifest_id}' IS NULL
            OR parameters #>> '{predecessor_run_fingerprint}' IS NULL
            OR parameters #>> '{predecessor_result_artifact_sha256}' IS NULL
            OR parameters #>> '{predecessor_input_lineage_sha256}' IS NULL
            OR parameters #>> '{predecessor_cell_summaries_sha256}' IS NULL
            OR parameters #>>
                '{predecessor_detail_shard_manifest_sha256}' IS NULL
            OR parameters #>> '{predecessor_final_checkpoint_sha256}' IS NULL) THEN
        RAISE EXCEPTION 'p1_05 completion predecessor parameters are incomplete';
    END IF;
    IF NEW.status IS DISTINCT FROM 'SUCCEEDED' THEN
        RETURN NEW;
    END IF;

    SELECT metadata INTO STRICT artifact_metadata
    FROM systematic_fx.artifacts
    WHERE artifact_id = NEW.result_artifact_id;
    IF artifact_metadata #>> '{campaign_key}' IS DISTINCT FROM
            'phase1a_conservative_screening_v1'
       OR artifact_metadata #>> '{query_id}' IS DISTINCT FROM NEW.pattern_key
       OR artifact_metadata #>> '{outcome_config_id}' IS DISTINCT FROM expected_config_id
       OR artifact_metadata #>> '{run_fingerprint}' IS DISTINCT FROM NEW.run_fingerprint
       OR artifact_metadata #>> '{source_artifact_manifest_sha256}' IS DISTINCT FROM
            NEW.source_artifact_manifest_sha256
       OR artifact_metadata #>> '{cell_summaries_sha256}' IS DISTINCT FROM
            NEW.cell_summaries_sha256
       OR artifact_metadata #>> '{summary_row_count}' IS DISTINCT FROM '2904'
       OR artifact_metadata #>> '{detail_record_count}' IS DISTINCT FROM
            expected_detail_count::text
       OR artifact_metadata #>> '{detail_shard_count}' IS DISTINCT FROM
            expected_planned_count::text
       OR artifact_metadata #>> '{planned_source_date_count}' IS DISTINCT FROM
            expected_planned_count::text
       OR COALESCE(artifact_metadata #>> '{cache_manifest_sha256}', '')
            !~ '^[0-9a-f]{64}$'
       OR COALESCE(artifact_metadata #>> '{detail_shard_manifest_sha256}', '')
            !~ '^[0-9a-f]{64}$'
       OR COALESCE(artifact_metadata #>> '{final_checkpoint_sha256}', '')
            !~ '^[0-9a-f]{64}$'
       OR COALESCE(artifact_metadata #>> '{input_lineage_sha256}', '')
            !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'ordered Phase 1A outcome completion lineage drift';
    END IF;
    IF NEW.pattern_key = 'p1_05_unconfirmed_move_reversal'
       AND (artifact_metadata #>> '{predecessor_equivalence_audit_id}'
                IS DISTINCT FROM
                parameters #>> '{predecessor_equivalence_audit_id}'
            OR artifact_metadata #>>
                '{predecessor_equivalence_audit_artifact_sha256}'
                IS DISTINCT FROM parameters #>>
                '{predecessor_equivalence_audit_artifact_sha256}'
            OR artifact_metadata #>> '{predecessor_outcome_replay_manifest_id}'
                IS DISTINCT FROM
                parameters #>> '{predecessor_outcome_replay_manifest_id}'
            OR artifact_metadata #>> '{predecessor_run_fingerprint}'
                IS DISTINCT FROM parameters #>> '{predecessor_run_fingerprint}'
            OR artifact_metadata #>> '{predecessor_result_artifact_sha256}'
                IS DISTINCT FROM
                parameters #>> '{predecessor_result_artifact_sha256}'
            OR artifact_metadata #>> '{predecessor_input_lineage_sha256}'
                IS DISTINCT FROM
                parameters #>> '{predecessor_input_lineage_sha256}'
            OR artifact_metadata #>> '{predecessor_cell_summaries_sha256}'
                IS DISTINCT FROM
                parameters #>> '{predecessor_cell_summaries_sha256}'
            OR artifact_metadata #>>
                '{predecessor_detail_shard_manifest_sha256}' IS DISTINCT FROM
                parameters #>> '{predecessor_detail_shard_manifest_sha256}'
            OR artifact_metadata #>> '{predecessor_final_checkpoint_sha256}'
                IS DISTINCT FROM
                parameters #>> '{predecessor_final_checkpoint_sha256}') THEN
        RAISE EXCEPTION 'p1_05 ordered completion predecessor lineage drift';
    END IF;

    SELECT count(*)::integer INTO observed_checkpoint_count
    FROM systematic_fx.phase1a_outcome_replay_checkpoints
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
      AND run_fingerprint = NEW.run_fingerprint;
    IF observed_checkpoint_count IS DISTINCT FROM expected_planned_count THEN
        RAISE EXCEPTION 'ordered Phase 1A outcome requires every source-date checkpoint';
    END IF;
    SELECT checkpoint_sequence, completed_source_date_count,
           last_completed_source_date, checkpoint_artifact_sha256,
           progress_metadata
    INTO STRICT final_checkpoint
    FROM systematic_fx.phase1a_outcome_replay_checkpoints
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
      AND run_fingerprint = NEW.run_fingerprint
    ORDER BY checkpoint_sequence DESC LIMIT 1;
    IF final_checkpoint.checkpoint_sequence IS DISTINCT FROM expected_planned_count
       OR final_checkpoint.completed_source_date_count IS DISTINCT FROM expected_planned_count
       OR final_checkpoint.last_completed_source_date IS DISTINCT FROM DATE '2023-08-31'
       OR final_checkpoint.checkpoint_artifact_sha256 IS DISTINCT FROM
            artifact_metadata #>> '{final_checkpoint_sha256}'
       OR final_checkpoint.progress_metadata #>> '{artifact_schema}' IS DISTINCT FROM
            'systematic_fx.phase1a_outcome_progress.v1'
       OR final_checkpoint.progress_metadata #>> '{replay_finished}' IS DISTINCT FROM 'true'
       OR final_checkpoint.progress_metadata #>> '{detail_record_count}' IS DISTINCT FROM
            expected_detail_count::text
       OR final_checkpoint.progress_metadata #>> '{detail_shard_count}' IS DISTINCT FROM
            expected_planned_count::text
       OR final_checkpoint.progress_metadata #>> '{cache_manifest_sha256}' IS DISTINCT FROM
            artifact_metadata #>> '{cache_manifest_sha256}'
       OR final_checkpoint.progress_metadata #>> '{detail_shard_manifest_sha256}' IS DISTINCT FROM
            artifact_metadata #>> '{detail_shard_manifest_sha256}'
       OR final_checkpoint.progress_metadata #>> '{input_lineage_sha256}' IS DISTINCT FROM
            artifact_metadata #>> '{input_lineage_sha256}' THEN
        RAISE EXCEPTION 'ordered Phase 1A final checkpoint is incomplete or unbound';
    END IF;

    SELECT result_summary INTO STRICT attempt_summary
    FROM systematic_fx.research_run_attempts
    WHERE research_run_attempt_id = NEW.research_run_attempt_id
      AND research_run_spec_id = NEW.research_run_spec_id;
    IF attempt_summary #>> '{artifact_sha256}' IS DISTINCT FROM NEW.result_artifact_sha256
       OR attempt_summary #>> '{cell_summaries_sha256}' IS DISTINCT FROM
            NEW.cell_summaries_sha256
       OR attempt_summary #>> '{summary_row_count}' IS DISTINCT FROM '2904'
       OR attempt_summary #>> '{detail_record_count}' IS DISTINCT FROM
            expected_detail_count::text
       OR attempt_summary #>> '{detail_shard_count}' IS DISTINCT FROM
            expected_planned_count::text
       OR attempt_summary #>> '{planned_source_date_count}' IS DISTINCT FROM
            expected_planned_count::text
       OR attempt_summary #>> '{cache_manifest_sha256}' IS DISTINCT FROM
            artifact_metadata #>> '{cache_manifest_sha256}'
       OR attempt_summary #>> '{detail_shard_manifest_sha256}' IS DISTINCT FROM
            artifact_metadata #>> '{detail_shard_manifest_sha256}'
       OR attempt_summary #>> '{final_checkpoint_sha256}' IS DISTINCT FROM
            artifact_metadata #>> '{final_checkpoint_sha256}'
       OR attempt_summary #>> '{input_lineage_sha256}' IS DISTINCT FROM
            artifact_metadata #>> '{input_lineage_sha256}'
       OR attempt_summary #>> '{source_artifact_manifest_sha256}' IS DISTINCT FROM
            NEW.source_artifact_manifest_sha256 THEN
        RAISE EXCEPTION 'ordered Phase 1A outcome attempt completion drift';
    END IF;
    IF NEW.pattern_key = 'p1_05_unconfirmed_move_reversal'
       AND (attempt_summary #>> '{predecessor_equivalence_audit_id}'
                IS DISTINCT FROM
                parameters #>> '{predecessor_equivalence_audit_id}'
            OR attempt_summary #>>
                '{predecessor_equivalence_audit_artifact_sha256}'
                IS DISTINCT FROM parameters #>>
                '{predecessor_equivalence_audit_artifact_sha256}'
            OR attempt_summary #>> '{predecessor_outcome_replay_manifest_id}'
                IS DISTINCT FROM
                parameters #>> '{predecessor_outcome_replay_manifest_id}'
            OR attempt_summary #>> '{predecessor_run_fingerprint}'
                IS DISTINCT FROM parameters #>> '{predecessor_run_fingerprint}'
            OR attempt_summary #>> '{predecessor_result_artifact_sha256}'
                IS DISTINCT FROM
                parameters #>> '{predecessor_result_artifact_sha256}'
            OR attempt_summary #>> '{predecessor_input_lineage_sha256}'
                IS DISTINCT FROM
                parameters #>> '{predecessor_input_lineage_sha256}'
            OR attempt_summary #>> '{predecessor_cell_summaries_sha256}'
                IS DISTINCT FROM
                parameters #>> '{predecessor_cell_summaries_sha256}'
            OR attempt_summary #>>
                '{predecessor_detail_shard_manifest_sha256}' IS DISTINCT FROM
                parameters #>> '{predecessor_detail_shard_manifest_sha256}'
            OR attempt_summary #>> '{predecessor_final_checkpoint_sha256}'
                IS DISTINCT FROM
                parameters #>> '{predecessor_final_checkpoint_sha256}') THEN
        RAISE EXCEPTION 'p1_05 attempt completion predecessor lineage drift';
    END IF;
    RETURN NEW;
END;
$$;


CREATE OR REPLACE FUNCTION systematic_fx.protect_phase1a_outcome_manifest()
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

    IF campaign_key_value IS DISTINCT FROM 'phase1a_conservative_screening_v1'
       OR spec_kind IS DISTINCT FROM 'OUTCOME_BUILD'
       OR spec_engine IS DISTINCT FROM 'phase1a_shared_outcome_replay_v1'
       OR spec_direction IS DISTINCT FROM 'BOTH'
       OR spec_experiment_id IS NOT NULL THEN
        RAISE EXCEPTION 'outcome replay must belong to the campaign-level Phase 1A p5 RunSpec';
    END IF;
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
       OR spec_parameters #> '{scenario_ids}' IS DISTINCT FROM
            '["BASELINE", "MODERATE_COMBINED", "SEVERE_DIAGNOSTIC"]'::jsonb
       OR spec_parameters #> '{direction_ids}' IS DISTINCT FROM '["LONG", "SHORT"]'::jsonb
       OR spec_parameters #> '{take_profit_ticks}' IS DISTINCT FROM
            '[24,32,40,48,56,64,72,80,88,96,104,112,120,128,136,144,152,160,168,176,184,192]'::jsonb
       OR spec_parameters #> '{stop_loss_ticks}' IS DISTINCT FROM
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

    IF attempt_status IS DISTINCT FROM NEW.status
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

        IF artifact_type_value IS DISTINCT FROM 'PHASE1A_OUTCOME_REPLAY_RESULT'
           OR position('/data/derived/' IN artifact_uri_value) = 0
           OR artifact_sha256_value IS DISTINCT FROM NEW.result_artifact_sha256
           OR artifact_byte_size_value IS DISTINCT FROM NEW.result_artifact_byte_size
           OR artifact_metadata_value #>> '{campaign_key}' IS DISTINCT FROM
                'phase1a_conservative_screening_v1'
           OR artifact_metadata_value #>> '{query_id}' IS DISTINCT FROM
                'p5_01_range_expansion_flow_continuation'
           OR artifact_metadata_value #>> '{outcome_config_id}' IS DISTINCT FROM
                'phase1a_p5_outcome_replay_v1'
           OR artifact_metadata_value #>> '{run_fingerprint}' IS DISTINCT FROM NEW.run_fingerprint
           OR artifact_metadata_value #>> '{source_artifact_manifest_sha256}' IS DISTINCT FROM
                NEW.source_artifact_manifest_sha256
           OR artifact_metadata_value #>> '{cell_summaries_sha256}' IS DISTINCT FROM
                NEW.cell_summaries_sha256
           OR artifact_metadata_value #>> '{summary_row_count}' IS DISTINCT FROM '2904' THEN
            RAISE EXCEPTION 'Phase 1A outcome replay result artifact lineage drift';
        END IF;

        SELECT count(*)::integer
        INTO observed_summary_count
        FROM systematic_fx.phase1a_outcome_cell_summaries
        WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
          AND run_fingerprint = NEW.run_fingerprint;
        IF observed_summary_count IS DISTINCT FROM NEW.expected_summary_count THEN
            RAISE EXCEPTION 'Phase 1A outcome replay requires all 2904 cell summaries';
        END IF;
        IF attempt_summary #>> '{artifact_sha256}' IS DISTINCT FROM NEW.result_artifact_sha256
           OR attempt_summary #>> '{cell_summaries_sha256}' IS DISTINCT FROM NEW.cell_summaries_sha256
           OR attempt_summary #>> '{summary_row_count}' IS DISTINCT FROM '2904'
           OR attempt_summary #>> '{source_artifact_manifest_sha256}' IS DISTINCT FROM
                NEW.source_artifact_manifest_sha256 THEN
            RAISE EXCEPTION 'Phase 1A outcome replay attempt summary drift';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION systematic_fx.protect_phase1a_outcome_checkpoint()
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
    IF TG_OP IS DISTINCT FROM 'INSERT' THEN
        RAISE EXCEPTION 'Phase 1A outcome replay checkpoints are append-only';
    END IF;
    SELECT run_fingerprint, status
    INTO STRICT manifest_fingerprint, manifest_status
    FROM systematic_fx.phase1a_outcome_replay_manifests
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id;
    IF manifest_fingerprint IS DISTINCT FROM NEW.run_fingerprint THEN
        RAISE EXCEPTION 'checkpoint run fingerprint differs from its outcome replay';
    END IF;
    IF manifest_status IS DISTINCT FROM 'RUNNING' THEN
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
        IF NEW.checkpoint_sequence IS DISTINCT FROM 1
           OR NEW.completed_source_date_count IS DISTINCT FROM 1
           OR NEW.predecessor_checkpoint_sha256 IS NOT NULL THEN
            RAISE EXCEPTION 'first outcome checkpoint must be source-date sequence 1';
        END IF;
    ELSIF NEW.checkpoint_sequence IS DISTINCT FROM previous_sequence + 1
          OR NEW.completed_source_date_count IS DISTINCT FROM previous_source_date_count + 1
          OR NEW.last_completed_source_date <= previous_source_date
          OR NEW.source_event_count < previous_source_event_count
          OR NEW.predecessor_checkpoint_sha256 IS DISTINCT FROM previous_artifact_sha256 THEN
        RAISE EXCEPTION 'outcome replay checkpoints must form one monotonic hash chain';
    END IF;

    SELECT artifact_type, uri, sha256, byte_size, metadata
    INTO STRICT artifact_type_value, artifact_uri_value, artifact_sha256_value,
                artifact_byte_size_value, artifact_metadata_value
    FROM systematic_fx.artifacts
    WHERE artifact_id = NEW.checkpoint_artifact_id;
    IF artifact_type_value IS DISTINCT FROM 'PHASE1A_OUTCOME_REPLAY_CHECKPOINT'
       OR position('/data/derived/' IN artifact_uri_value) = 0
       OR artifact_sha256_value IS DISTINCT FROM NEW.checkpoint_artifact_sha256
       OR artifact_byte_size_value IS DISTINCT FROM NEW.checkpoint_artifact_byte_size
       OR artifact_metadata_value #>> '{run_fingerprint}' IS DISTINCT FROM NEW.run_fingerprint
       OR artifact_metadata_value #>> '{checkpoint_sequence}' IS DISTINCT FROM
            NEW.checkpoint_sequence::text
       OR artifact_metadata_value #>> '{last_completed_source_date}' IS DISTINCT FROM
            NEW.last_completed_source_date::text THEN
        RAISE EXCEPTION 'outcome replay checkpoint artifact lineage drift';
    END IF;
    RETURN NEW;
END;
$$;

ALTER TABLE systematic_fx.phase1a_outcome_replay_equivalence_audits
    ADD CONSTRAINT phase1a_outcome_equivalence_one_per_subject
    UNIQUE (predecessor_outcome_replay_manifest_id);

COMMENT ON CONSTRAINT phase1a_outcome_equivalence_one_per_subject
ON systematic_fx.phase1a_outcome_replay_equivalence_audits IS
    'One byte-verified PASSED equivalence proof is canonical for each immutable p5 replay subject.';

INSERT INTO systematic_fx.schema_migrations(version, name, checksum)
VALUES (
    19,
    'phase1a_outcome_audit_lineage_hardening',
    :'migration_checksum'
);

COMMIT;
