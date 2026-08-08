BEGIN;

ALTER TABLE systematic_fx.phase1a_outcome_replay_manifests
    DROP CONSTRAINT phase1a_outcome_manifests_fixed_identity,
    DROP CONSTRAINT phase1a_outcome_manifests_frozen_completion_plan,
    ADD CONSTRAINT phase1a_outcome_manifests_ordered_candidate_identity
        CHECK (
            (pattern_key = 'p5_01_range_expansion_flow_continuation'
             AND source_slice_count = 99
             AND source_occurrence_count = 1111
             AND expected_detail_record_count = 1613172
             AND planned_source_date_count = 485
             AND final_source_date = DATE '2023-08-31')
            OR
            (pattern_key = 'p1_05_unconfirmed_move_reversal'
             AND source_slice_count = 99
             AND source_occurrence_count = 943
             AND expected_detail_record_count = 1369236
             AND planned_source_date_count = 478
             AND final_source_date = DATE '2023-08-31')
        ),
    ADD CONSTRAINT phase1a_outcome_manifests_shared_surface_identity
        CHECK (scenario_count = 3
               AND direction_count = 2
               AND barrier_axis_size = 22
               AND cell_count_per_surface = 484
               AND expected_summary_count = 2904);

ALTER TABLE systematic_fx.phase1a_outcome_cell_summaries
    DROP CONSTRAINT phase1a_outcome_cells_frozen_signal_count;

CREATE TABLE systematic_fx.phase1a_outcome_replay_equivalence_audits (
    outcome_equivalence_audit_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    predecessor_outcome_replay_manifest_id bigint NOT NULL,
    campaign_id bigint NOT NULL,
    validation_research_run_spec_id bigint NOT NULL,
    validation_research_run_attempt_id bigint NOT NULL UNIQUE,
    validation_run_fingerprint text NOT NULL,
    audit_artifact_id bigint NOT NULL UNIQUE,
    audit_artifact_sha256 text NOT NULL,
    audit_artifact_byte_size bigint NOT NULL,
    predecessor_run_fingerprint text NOT NULL,
    predecessor_result_artifact_sha256 text NOT NULL,
    uninterrupted_result_artifact_sha256 text NOT NULL,
    resumed_result_artifact_sha256 text NOT NULL,
    cache_manifest_sha256 text NOT NULL,
    cell_summaries_sha256 text NOT NULL,
    detail_shard_manifest_sha256 text NOT NULL,
    input_lineage_sha256 text NOT NULL,
    final_checkpoint_sha256 text NOT NULL,
    checkpoint_chain_sha256 text NOT NULL,
    checkpoint_count integer NOT NULL,
    passed boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT phase1a_outcome_equivalence_subject_fk
        FOREIGN KEY (predecessor_outcome_replay_manifest_id)
        REFERENCES systematic_fx.phase1a_outcome_replay_manifests
            (outcome_replay_manifest_id),
    CONSTRAINT phase1a_outcome_equivalence_attempt_fk
        FOREIGN KEY (validation_research_run_attempt_id,
                     validation_research_run_spec_id)
        REFERENCES systematic_fx.research_run_attempts
            (research_run_attempt_id, research_run_spec_id),
    CONSTRAINT phase1a_outcome_equivalence_spec_fk
        FOREIGN KEY (validation_research_run_spec_id, campaign_id,
                     validation_run_fingerprint)
        REFERENCES systematic_fx.research_run_specs
            (research_run_spec_id, campaign_id, run_fingerprint),
    CONSTRAINT phase1a_outcome_equivalence_artifact_fk
        FOREIGN KEY (audit_artifact_id)
        REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT phase1a_outcome_equivalence_hashes_valid
        CHECK (validation_run_fingerprint ~ '^[0-9a-f]{64}$'
               AND audit_artifact_sha256 ~ '^[0-9a-f]{64}$'
               AND predecessor_run_fingerprint ~ '^[0-9a-f]{64}$'
               AND predecessor_result_artifact_sha256 ~ '^[0-9a-f]{64}$'
               AND uninterrupted_result_artifact_sha256 ~ '^[0-9a-f]{64}$'
               AND resumed_result_artifact_sha256 ~ '^[0-9a-f]{64}$'
               AND cache_manifest_sha256 ~ '^[0-9a-f]{64}$'
               AND cell_summaries_sha256 ~ '^[0-9a-f]{64}$'
               AND detail_shard_manifest_sha256 ~ '^[0-9a-f]{64}$'
               AND input_lineage_sha256 ~ '^[0-9a-f]{64}$'
               AND final_checkpoint_sha256 ~ '^[0-9a-f]{64}$'
               AND checkpoint_chain_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT phase1a_outcome_equivalence_size_count_valid
        CHECK (audit_artifact_byte_size > 0 AND checkpoint_count = 485),
    CONSTRAINT phase1a_outcome_equivalence_only_passes
        CHECK (passed
               AND uninterrupted_result_artifact_sha256 =
                    predecessor_result_artifact_sha256
               AND resumed_result_artifact_sha256 =
                    predecessor_result_artifact_sha256)
);

CREATE TABLE systematic_fx.phase1a_outcome_screening_decisions (
    outcome_replay_manifest_id bigint NOT NULL,
    direction text NOT NULL,
    decision_label text NOT NULL,
    selected_take_profit_ticks integer,
    selected_stop_loss_ticks integer,
    positive_region_size integer NOT NULL,
    rejection_reasons jsonb NOT NULL,
    decision_sha256 text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (outcome_replay_manifest_id, direction),
    CONSTRAINT phase1a_outcome_decisions_manifest_fk
        FOREIGN KEY (outcome_replay_manifest_id)
        REFERENCES systematic_fx.phase1a_outcome_replay_manifests
            (outcome_replay_manifest_id),
    CONSTRAINT phase1a_outcome_decisions_direction_valid
        CHECK (direction IN ('LONG', 'SHORT')),
    CONSTRAINT phase1a_outcome_decisions_label_valid
        CHECK (decision_label IN ('SCREENING_REJECT', 'SCREENING_SURVIVOR')),
    CONSTRAINT phase1a_outcome_decisions_selected_pair
        CHECK ((selected_take_profit_ticks IS NULL) =
               (selected_stop_loss_ticks IS NULL)
               AND (decision_label <> 'SCREENING_SURVIVOR'
                    OR selected_take_profit_ticks IS NOT NULL)
               AND (selected_take_profit_ticks IS NULL
                    OR (selected_take_profit_ticks BETWEEN 24 AND 192
                        AND (selected_take_profit_ticks - 24) % 8 = 0
                        AND selected_stop_loss_ticks BETWEEN 24 AND 192
                        AND (selected_stop_loss_ticks - 24) % 8 = 0))),
    CONSTRAINT phase1a_outcome_decisions_region_valid
        CHECK (positive_region_size >= 0),
    CONSTRAINT phase1a_outcome_decisions_reasons_valid
        CHECK (jsonb_typeof(rejection_reasons) = 'array'
               AND (decision_label <> 'SCREENING_REJECT'
                    OR jsonb_array_length(rejection_reasons) > 0)),
    CONSTRAINT phase1a_outcome_decisions_sha256_valid
        CHECK (decision_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE FUNCTION systematic_fx.protect_phase1a_outcome_equivalence_audit()
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
    IF TG_OP <> 'INSERT' THEN
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
    IF subject.pattern_key <> 'p5_01_range_expansion_flow_continuation'
       OR subject.status <> 'SUCCEEDED'
       OR subject.campaign_id <> NEW.campaign_id
       OR subject.run_fingerprint <> NEW.predecessor_run_fingerprint
       OR subject.result_artifact_sha256 <>
            NEW.predecessor_result_artifact_sha256
       OR subject.cell_summaries_sha256 <> NEW.cell_summaries_sha256
       OR subject_metadata #>> '{cache_manifest_sha256}' <>
            NEW.cache_manifest_sha256
       OR subject_metadata #>> '{detail_shard_manifest_sha256}' <>
            NEW.detail_shard_manifest_sha256
       OR subject_metadata #>> '{input_lineage_sha256}' <>
            NEW.input_lineage_sha256
       OR subject_metadata #>> '{final_checkpoint_sha256}' <>
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
    IF campaign_key_value <> 'phase1a_conservative_screening_v1'
       OR validation_spec.run_kind <> 'VALIDATION'
       OR validation_spec.engine_version <>
            'phase1a_outcome_equivalence_audit_v1'
       OR validation_spec.direction <> 'BOTH'
       OR validation_spec.experiment_id IS NOT NULL
       OR validation_parameters #>> '{audit_kind}' <>
            'UNINTERRUPTED_VS_RESUMED_BYTE_EQUIVALENCE'
       OR validation_parameters #>> '{query_id}' <>
            'p5_01_range_expansion_flow_continuation'
       OR validation_parameters #>> '{predecessor_outcome_replay_manifest_id}' <>
            NEW.predecessor_outcome_replay_manifest_id::text
       OR validation_parameters #>> '{predecessor_run_fingerprint}' <>
            NEW.predecessor_run_fingerprint
       OR validation_parameters #>> '{predecessor_result_artifact_sha256}' <>
            NEW.predecessor_result_artifact_sha256
       OR validation_parameters #>> '{cache_manifest_sha256}' <>
            NEW.cache_manifest_sha256
       OR validation_parameters #>> '{cell_summaries_sha256}' <>
            NEW.cell_summaries_sha256
       OR validation_parameters #>> '{detail_shard_manifest_sha256}' <>
            NEW.detail_shard_manifest_sha256
       OR validation_parameters #>> '{input_lineage_sha256}' <>
            NEW.input_lineage_sha256
       OR validation_parameters #>> '{final_checkpoint_sha256}' <>
            NEW.final_checkpoint_sha256
       OR validation_parameters #>> '{checkpoint_chain_sha256}' <>
            NEW.checkpoint_chain_sha256
       OR validation_parameters #>> '{checkpoint_count}' <>
            NEW.checkpoint_count::text THEN
        RAISE EXCEPTION 'p5 equivalence audit validation RunSpec drift';
    END IF;

    SELECT status, result_artifact_id, result_summary
    INTO STRICT validation_attempt
    FROM systematic_fx.research_run_attempts
    WHERE research_run_attempt_id = NEW.validation_research_run_attempt_id
      AND research_run_spec_id = NEW.validation_research_run_spec_id;
    IF validation_attempt.status <> 'SUCCEEDED'
       OR validation_attempt.result_artifact_id <> NEW.audit_artifact_id
       OR validation_attempt.result_summary #>> '{audit_artifact_sha256}' <>
            NEW.audit_artifact_sha256
       OR validation_attempt.result_summary #>> '{checkpoint_chain_sha256}' <>
            NEW.checkpoint_chain_sha256
       OR validation_attempt.result_summary #>> '{passed}' <> 'true' THEN
        RAISE EXCEPTION 'p5 equivalence audit validation attempt drift';
    END IF;

    SELECT artifact_type, uri, sha256, byte_size, media_type, metadata
    INTO STRICT audit_artifact
    FROM systematic_fx.artifacts
    WHERE artifact_id = NEW.audit_artifact_id;
    IF audit_artifact.artifact_type <>
            'PHASE1A_OUTCOME_REPLAY_EQUIVALENCE_AUDIT'
       OR position('/data/derived/' IN audit_artifact.uri) = 0
       OR audit_artifact.sha256 <> NEW.audit_artifact_sha256
       OR audit_artifact.byte_size <> NEW.audit_artifact_byte_size
       OR audit_artifact.media_type <> 'application/json'
       OR audit_artifact.metadata #>> '{campaign_key}' <>
            'phase1a_conservative_screening_v1'
       OR audit_artifact.metadata #>> '{audit_kind}' <>
            'UNINTERRUPTED_VS_RESUMED_BYTE_EQUIVALENCE'
       OR audit_artifact.metadata #>> '{predecessor_outcome_replay_manifest_id}' <>
            NEW.predecessor_outcome_replay_manifest_id::text
       OR audit_artifact.metadata #>> '{predecessor_result_artifact_sha256}' <>
            NEW.predecessor_result_artifact_sha256
       OR audit_artifact.metadata #>> '{checkpoint_chain_sha256}' <>
            NEW.checkpoint_chain_sha256
       OR audit_artifact.metadata #>> '{validation_run_fingerprint}' <>
            NEW.validation_run_fingerprint
       OR audit_artifact.metadata #>> '{passed}' <> 'true' THEN
        RAISE EXCEPTION 'p5 equivalence audit artifact lineage drift';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER phase1a_outcome_equivalence_audits_append_only
BEFORE INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_outcome_replay_equivalence_audits
FOR EACH ROW EXECUTE FUNCTION
    systematic_fx.protect_phase1a_outcome_equivalence_audit();

CREATE FUNCTION systematic_fx.protect_phase1a_outcome_screening_decision()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    manifest_status text;
    observed_summary_count integer;
    selected_cell_exists boolean;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Phase 1A outcome screening decisions are append-only';
    END IF;
    SELECT status INTO STRICT manifest_status
    FROM systematic_fx.phase1a_outcome_replay_manifests
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id;
    SELECT count(*)::integer INTO observed_summary_count
    FROM systematic_fx.phase1a_outcome_cell_summaries
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id;
    IF manifest_status <> 'SUCCEEDED' OR observed_summary_count <> 2904 THEN
        RAISE EXCEPTION 'screening decisions require one complete successful outcome surface';
    END IF;
    IF NEW.selected_take_profit_ticks IS NOT NULL THEN
        SELECT EXISTS (
            SELECT 1
            FROM systematic_fx.phase1a_outcome_cell_summaries
            WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
              AND direction = NEW.direction
              AND take_profit_ticks = NEW.selected_take_profit_ticks
              AND stop_loss_ticks = NEW.selected_stop_loss_ticks
        ) INTO selected_cell_exists;
        IF NOT selected_cell_exists THEN
            RAISE EXCEPTION 'selected screening cell is absent from the outcome surface';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER phase1a_outcome_screening_decisions_append_only
BEFORE INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_outcome_screening_decisions
FOR EACH ROW EXECUTE FUNCTION
    systematic_fx.protect_phase1a_outcome_screening_decision();

CREATE FUNCTION systematic_fx.protect_phase1a_ordered_outcome_manifest()
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
    IF spec.campaign_key <> 'phase1a_conservative_screening_v1'
       OR spec.run_kind <> 'OUTCOME_BUILD'
       OR spec.engine_version <> 'phase1a_shared_outcome_replay_v1'
       OR spec.direction <> 'BOTH'
       OR spec.experiment_id IS NOT NULL
       OR spec_parameters #>> '{query_id}' <> NEW.pattern_key
       OR spec_parameters #>> '{outcome_config_id}' <> expected_config_id
       OR spec_parameters #>> '{source_artifact_manifest_sha256}' <>
            NEW.source_artifact_manifest_sha256
       OR spec_parameters #>> '{source_slice_count}' <> '99'
       OR spec_parameters #>> '{source_occurrence_count}' <>
            expected_occurrences::text
       OR spec_parameters #>> '{cell_count_per_surface}' <> '484'
       OR spec_parameters #>> '{expected_summary_count}' <> '2904'
       OR spec_parameters #>> '{expected_detail_record_count}' <>
            expected_detail_count::text
       OR spec_parameters #>> '{planned_source_date_count}' <>
            expected_planned_count::text
       OR spec_parameters #>> '{final_source_date}' <> '2023-08-31'
       OR spec_parameters #> '{expected_direction_signal_counts}' <>
            jsonb_build_object('LONG', expected_long_count,
                               'SHORT', expected_short_count)
       OR spec_parameters #> '{scenario_ids}' <>
            '["BASELINE","MODERATE_COMBINED","SEVERE_DIAGNOSTIC"]'::jsonb
       OR spec_parameters #> '{direction_ids}' <> '["LONG","SHORT"]'::jsonb
       OR spec_parameters #> '{take_profit_ticks}' <>
            '[24,32,40,48,56,64,72,80,88,96,104,112,120,128,136,144,152,160,168,176,184,192]'::jsonb
       OR spec_parameters #> '{stop_loss_ticks}' <>
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
        IF spec_parameters #>> '{predecessor_equivalence_audit_artifact_sha256}' <>
                predecessor_audit.audit_artifact_sha256
           OR spec_parameters #>> '{predecessor_outcome_replay_manifest_id}' <>
                predecessor_audit.predecessor_outcome_replay_manifest_id::text
           OR spec_parameters #>> '{predecessor_run_fingerprint}' <>
                predecessor_audit.predecessor_run_fingerprint
           OR spec_parameters #>> '{predecessor_result_artifact_sha256}' <>
                predecessor_audit.predecessor_result_artifact_sha256
           OR spec_parameters #>> '{predecessor_input_lineage_sha256}' <>
                predecessor_audit.input_lineage_sha256
           OR spec_parameters #>> '{predecessor_cell_summaries_sha256}' <>
                predecessor_audit.cell_summaries_sha256
           OR spec_parameters #>> '{predecessor_detail_shard_manifest_sha256}' <>
                predecessor_audit.detail_shard_manifest_sha256
           OR spec_parameters #>> '{predecessor_final_checkpoint_sha256}' <>
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
    IF attempt.status <> NEW.status
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
        IF artifact.artifact_type <> 'PHASE1A_OUTCOME_REPLAY_RESULT'
           OR position('/data/derived/' IN artifact.uri) = 0
           OR artifact.sha256 <> NEW.result_artifact_sha256
           OR artifact.byte_size <> NEW.result_artifact_byte_size
           OR artifact.metadata #>> '{campaign_key}' <>
                'phase1a_conservative_screening_v1'
           OR artifact.metadata #>> '{query_id}' <> NEW.pattern_key
           OR artifact.metadata #>> '{outcome_config_id}' <> expected_config_id
           OR artifact.metadata #>> '{run_fingerprint}' <> NEW.run_fingerprint
           OR artifact.metadata #>> '{source_artifact_manifest_sha256}' <>
                NEW.source_artifact_manifest_sha256
           OR artifact.metadata #>> '{cell_summaries_sha256}' <>
                NEW.cell_summaries_sha256
           OR artifact.metadata #>> '{summary_row_count}' <> '2904' THEN
            RAISE EXCEPTION 'ordered Phase 1A outcome result artifact lineage drift';
        END IF;
        IF NEW.pattern_key = 'p1_05_unconfirmed_move_reversal'
           AND (artifact.metadata #>> '{predecessor_equivalence_audit_id}' <>
                    predecessor_audit.outcome_equivalence_audit_id::text
                OR artifact.metadata #>>
                    '{predecessor_equivalence_audit_artifact_sha256}' <>
                    predecessor_audit.audit_artifact_sha256) THEN
            RAISE EXCEPTION 'p1_05 result predecessor audit lineage drift';
        END IF;
        SELECT count(*)::integer INTO observed_summary_count
        FROM systematic_fx.phase1a_outcome_cell_summaries
        WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
          AND run_fingerprint = NEW.run_fingerprint;
        IF observed_summary_count <> 2904 THEN
            RAISE EXCEPTION 'Phase 1A outcome replay requires all 2904 cell summaries';
        END IF;
        IF attempt.result_summary #>> '{artifact_sha256}' <>
                NEW.result_artifact_sha256
           OR attempt.result_summary #>> '{cell_summaries_sha256}' <>
                NEW.cell_summaries_sha256
           OR attempt.result_summary #>> '{summary_row_count}' <> '2904'
           OR attempt.result_summary #>> '{source_artifact_manifest_sha256}' <>
                NEW.source_artifact_manifest_sha256 THEN
            RAISE EXCEPTION 'Phase 1A outcome replay attempt summary drift';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER phase1a_outcome_manifests_preserve_and_validate
ON systematic_fx.phase1a_outcome_replay_manifests;
CREATE TRIGGER phase1a_outcome_manifests_preserve_and_validate
BEFORE INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_outcome_replay_manifests
FOR EACH ROW EXECUTE FUNCTION
    systematic_fx.protect_phase1a_ordered_outcome_manifest();

CREATE FUNCTION systematic_fx.harden_phase1a_ordered_outcome_completion()
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
    IF parameters #>> '{query_id}' <> NEW.pattern_key
       OR parameters #>> '{outcome_config_id}' <> expected_config_id
       OR parameters #>> '{source_artifact_manifest_sha256}' <>
            NEW.source_artifact_manifest_sha256
       OR parameters #>> '{source_slice_count}' <> '99'
       OR parameters #>> '{source_occurrence_count}' <>
            expected_occurrences::text
       OR parameters #>> '{cell_count_per_surface}' <> '484'
       OR parameters #>> '{expected_summary_count}' <> '2904'
       OR parameters #>> '{expected_detail_record_count}' <>
            expected_detail_count::text
       OR parameters #>> '{planned_source_date_count}' <>
            expected_planned_count::text
       OR parameters #>> '{expected_completed_source_date_count}' <>
            expected_planned_count::text
       OR parameters #>> '{expected_last_completed_source_date}' <>
            '2023-08-31'
       OR parameters #> '{expected_direction_signal_counts}' <>
            jsonb_build_object('LONG', expected_long_count,
                               'SHORT', expected_short_count)
       OR parameters #> '{scenario_cost_ticks_per_fill}' <>
            '{"BASELINE":{"allocated_fixed":4,"variable":4},"MODERATE_COMBINED":{"allocated_fixed":5,"variable":5},"SEVERE_DIAGNOSTIC":{"allocated_fixed":6,"variable":6}}'::jsonb THEN
        RAISE EXCEPTION 'ordered Phase 1A outcome completion parameter drift';
    END IF;
    IF NEW.status <> 'SUCCEEDED' THEN
        RETURN NEW;
    END IF;

    SELECT metadata INTO STRICT artifact_metadata
    FROM systematic_fx.artifacts
    WHERE artifact_id = NEW.result_artifact_id;
    IF artifact_metadata #>> '{campaign_key}' <>
            'phase1a_conservative_screening_v1'
       OR artifact_metadata #>> '{query_id}' <> NEW.pattern_key
       OR artifact_metadata #>> '{outcome_config_id}' <> expected_config_id
       OR artifact_metadata #>> '{run_fingerprint}' <> NEW.run_fingerprint
       OR artifact_metadata #>> '{source_artifact_manifest_sha256}' <>
            NEW.source_artifact_manifest_sha256
       OR artifact_metadata #>> '{cell_summaries_sha256}' <>
            NEW.cell_summaries_sha256
       OR artifact_metadata #>> '{summary_row_count}' <> '2904'
       OR artifact_metadata #>> '{detail_record_count}' <>
            expected_detail_count::text
       OR artifact_metadata #>> '{detail_shard_count}' <>
            expected_planned_count::text
       OR artifact_metadata #>> '{planned_source_date_count}' <>
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

    SELECT count(*)::integer INTO observed_checkpoint_count
    FROM systematic_fx.phase1a_outcome_replay_checkpoints
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
      AND run_fingerprint = NEW.run_fingerprint;
    IF observed_checkpoint_count <> expected_planned_count THEN
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
    IF final_checkpoint.checkpoint_sequence <> expected_planned_count
       OR final_checkpoint.completed_source_date_count <> expected_planned_count
       OR final_checkpoint.last_completed_source_date <> DATE '2023-08-31'
       OR final_checkpoint.checkpoint_artifact_sha256 <>
            artifact_metadata #>> '{final_checkpoint_sha256}'
       OR final_checkpoint.progress_metadata #>> '{artifact_schema}' <>
            'systematic_fx.phase1a_outcome_progress.v1'
       OR final_checkpoint.progress_metadata #>> '{replay_finished}' <> 'true'
       OR final_checkpoint.progress_metadata #>> '{detail_record_count}' <>
            expected_detail_count::text
       OR final_checkpoint.progress_metadata #>> '{detail_shard_count}' <>
            expected_planned_count::text
       OR final_checkpoint.progress_metadata #>> '{cache_manifest_sha256}' <>
            artifact_metadata #>> '{cache_manifest_sha256}'
       OR final_checkpoint.progress_metadata #>> '{detail_shard_manifest_sha256}' <>
            artifact_metadata #>> '{detail_shard_manifest_sha256}'
       OR final_checkpoint.progress_metadata #>> '{input_lineage_sha256}' <>
            artifact_metadata #>> '{input_lineage_sha256}' THEN
        RAISE EXCEPTION 'ordered Phase 1A final checkpoint is incomplete or unbound';
    END IF;

    SELECT result_summary INTO STRICT attempt_summary
    FROM systematic_fx.research_run_attempts
    WHERE research_run_attempt_id = NEW.research_run_attempt_id
      AND research_run_spec_id = NEW.research_run_spec_id;
    IF attempt_summary #>> '{artifact_sha256}' <> NEW.result_artifact_sha256
       OR attempt_summary #>> '{cell_summaries_sha256}' <>
            NEW.cell_summaries_sha256
       OR attempt_summary #>> '{summary_row_count}' <> '2904'
       OR attempt_summary #>> '{detail_record_count}' <>
            expected_detail_count::text
       OR attempt_summary #>> '{detail_shard_count}' <>
            expected_planned_count::text
       OR attempt_summary #>> '{planned_source_date_count}' <>
            expected_planned_count::text
       OR attempt_summary #>> '{cache_manifest_sha256}' <>
            artifact_metadata #>> '{cache_manifest_sha256}'
       OR attempt_summary #>> '{detail_shard_manifest_sha256}' <>
            artifact_metadata #>> '{detail_shard_manifest_sha256}'
       OR attempt_summary #>> '{final_checkpoint_sha256}' <>
            artifact_metadata #>> '{final_checkpoint_sha256}'
       OR attempt_summary #>> '{input_lineage_sha256}' <>
            artifact_metadata #>> '{input_lineage_sha256}'
       OR attempt_summary #>> '{source_artifact_manifest_sha256}' <>
            NEW.source_artifact_manifest_sha256 THEN
        RAISE EXCEPTION 'ordered Phase 1A outcome attempt completion drift';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER phase1a_outcome_manifest_completion_hardening
ON systematic_fx.phase1a_outcome_replay_manifests;
CREATE TRIGGER phase1a_outcome_manifest_completion_hardening
BEFORE INSERT OR UPDATE
ON systematic_fx.phase1a_outcome_replay_manifests
FOR EACH ROW EXECUTE FUNCTION
    systematic_fx.harden_phase1a_ordered_outcome_completion();

CREATE FUNCTION systematic_fx.protect_phase1a_ordered_outcome_cell_summary()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    manifest record;
    expected_signal_count integer;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Phase 1A outcome cell summaries are append-only';
    END IF;
    SELECT run_fingerprint, status, pattern_key INTO STRICT manifest
    FROM systematic_fx.phase1a_outcome_replay_manifests
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id;
    IF manifest.run_fingerprint <> NEW.run_fingerprint
       OR manifest.status <> 'RUNNING' THEN
        RAISE EXCEPTION 'outcome cell summary must belong to its RUNNING replay';
    END IF;
    expected_signal_count := CASE
        WHEN manifest.pattern_key = 'p5_01_range_expansion_flow_continuation'
             AND NEW.direction = 'LONG' THEN 529
        WHEN manifest.pattern_key = 'p5_01_range_expansion_flow_continuation'
             AND NEW.direction = 'SHORT' THEN 582
        WHEN manifest.pattern_key = 'p1_05_unconfirmed_move_reversal'
             AND NEW.direction = 'LONG' THEN 446
        WHEN manifest.pattern_key = 'p1_05_unconfirmed_move_reversal'
             AND NEW.direction = 'SHORT' THEN 497
        ELSE NULL
    END;
    IF expected_signal_count IS NULL OR NEW.signal_count <> expected_signal_count THEN
        RAISE EXCEPTION 'outcome cell signal_count differs from its frozen query/direction';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER phase1a_outcome_cells_append_only
ON systematic_fx.phase1a_outcome_cell_summaries;
CREATE TRIGGER phase1a_outcome_cells_append_only
BEFORE INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_outcome_cell_summaries
FOR EACH ROW EXECUTE FUNCTION
    systematic_fx.protect_phase1a_ordered_outcome_cell_summary();

CREATE FUNCTION systematic_fx.require_phase1a_ordered_outcome_attempt_manifest()
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
          AND run_spec.canonical_spec #>> '{parameters,query_id}' IN
              ('p5_01_range_expansion_flow_continuation',
               'p1_05_unconfirmed_move_reversal')
    ) INTO governed;
    IF NOT governed OR NEW.status = 'SKIPPED_DUPLICATE' THEN
        RETURN NULL;
    END IF;
    IF NEW.status NOT IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED') THEN
        RAISE EXCEPTION 'governed Phase 1A outcome attempt status is invalid';
    END IF;
    SELECT status, result_artifact_id
    INTO manifest_status, manifest_artifact_id
    FROM systematic_fx.phase1a_outcome_replay_manifests
    WHERE research_run_attempt_id = NEW.research_run_attempt_id
      AND research_run_spec_id = NEW.research_run_spec_id;
    IF NOT FOUND
       OR manifest_status <> NEW.status
       OR manifest_artifact_id IS DISTINCT FROM NEW.result_artifact_id THEN
        RAISE EXCEPTION 'governed Phase 1A outcome attempt requires one matching manifest';
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER research_run_attempts_require_outcome_manifest
ON systematic_fx.research_run_attempts;
CREATE CONSTRAINT TRIGGER research_run_attempts_require_outcome_manifest
AFTER INSERT OR UPDATE
ON systematic_fx.research_run_attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION
    systematic_fx.require_phase1a_ordered_outcome_attempt_manifest();

CREATE OR REPLACE FUNCTION systematic_fx.reject_phase1a_outcome_artifact_mutation()
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
    ) OR EXISTS (
        SELECT 1
        FROM systematic_fx.phase1a_outcome_replay_equivalence_audits AS audit
        WHERE audit.audit_artifact_id = OLD.artifact_id
    ) THEN
        RAISE EXCEPTION 'Phase 1A outcome replay artifacts are immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER phase1a_outcome_equivalence_publication_refresh
AFTER INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_outcome_replay_equivalence_audits
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();

CREATE TRIGGER phase1a_outcome_decisions_publication_refresh
AFTER INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_outcome_screening_decisions
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();

COMMENT ON TABLE systematic_fx.phase1a_outcome_replay_manifests IS
    'Append-preserved p5 then p1_05 outcome attempts, each owned by a distinct canonical RunSpec.';
COMMENT ON TABLE systematic_fx.phase1a_outcome_replay_equivalence_audits IS
    'Append-only byte-equivalence proof that gates p1_05 after the successful p5 replay.';
COMMENT ON TABLE systematic_fx.phase1a_outcome_screening_decisions IS
    'Append-only per-direction economic screening decision over one complete 2,904-cell surface.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (16, 'phase1a_ordered_outcome_candidates', :'migration_checksum');

COMMIT;
