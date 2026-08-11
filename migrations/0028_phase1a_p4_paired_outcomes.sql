BEGIN;

ALTER TABLE systematic_fx.phase1a_outcome_replay_manifests
    DROP CONSTRAINT phase1a_outcome_manifests_ordered_candidate_identity,
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
            OR
            (pattern_key = 'p4_01_opposite_depth_depletion_continuation'
             AND source_slice_count = 99
             AND source_occurrence_count = 334
             AND expected_detail_record_count = 484968
             AND planned_source_date_count = 472
             AND final_source_date = DATE '2023-08-31')
            OR
            (pattern_key = 'p4_02_depth_resistance_reversal'
             AND source_slice_count = 99
             AND source_occurrence_count = 340
             AND expected_detail_record_count = 493680
             AND planned_source_date_count = 455
             AND final_source_date = DATE '2023-08-31')
        );

CREATE TABLE systematic_fx.phase1a_p4_outcome_pair_batches (
    p4_pair_batch_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pair_id text NOT NULL,
    p4_01_outcome_replay_manifest_id bigint NOT NULL UNIQUE,
    p4_02_outcome_replay_manifest_id bigint NOT NULL UNIQUE,
    p4_01_run_fingerprint text NOT NULL,
    p4_02_run_fingerprint text NOT NULL,
    pair_config_sha256 text NOT NULL,
    p4_01_outcome_config_sha256 text NOT NULL,
    p4_02_outcome_config_sha256 text NOT NULL,
    p4_01_query_definition_sha256 text NOT NULL,
    p4_02_query_definition_sha256 text NOT NULL,
    p4_01_signal_manifest_sha256 text NOT NULL,
    p4_02_signal_manifest_sha256 text NOT NULL,
    p4_01_input_plan_sha256 text NOT NULL,
    p4_02_input_plan_sha256 text NOT NULL,
    prior_outcome_lineage jsonb NOT NULL,
    prior_outcome_lineage_sha256 text NOT NULL,
    status text NOT NULL DEFAULT 'PREPARED',
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    finished_at timestamptz,
    error_message text,
    CONSTRAINT phase1a_p4_pair_batches_p4_01_fk
        FOREIGN KEY (p4_01_outcome_replay_manifest_id)
        REFERENCES systematic_fx.phase1a_outcome_replay_manifests
            (outcome_replay_manifest_id),
    CONSTRAINT phase1a_p4_pair_batches_p4_02_fk
        FOREIGN KEY (p4_02_outcome_replay_manifest_id)
        REFERENCES systematic_fx.phase1a_outcome_replay_manifests
            (outcome_replay_manifest_id),
    CONSTRAINT phase1a_p4_pair_batches_members_distinct
        CHECK (p4_01_outcome_replay_manifest_id <>
               p4_02_outcome_replay_manifest_id),
    CONSTRAINT phase1a_p4_pair_batches_hashes_valid
        CHECK (p4_01_run_fingerprint ~ '^[0-9a-f]{64}$'
               AND p4_02_run_fingerprint ~ '^[0-9a-f]{64}$'
               AND pair_config_sha256 ~ '^[0-9a-f]{64}$'
               AND p4_01_outcome_config_sha256 ~ '^[0-9a-f]{64}$'
               AND p4_02_outcome_config_sha256 ~ '^[0-9a-f]{64}$'
               AND p4_01_query_definition_sha256 ~ '^[0-9a-f]{64}$'
               AND p4_02_query_definition_sha256 ~ '^[0-9a-f]{64}$'
               AND p4_01_signal_manifest_sha256 ~ '^[0-9a-f]{64}$'
               AND p4_02_signal_manifest_sha256 ~ '^[0-9a-f]{64}$'
               AND p4_01_input_plan_sha256 ~ '^[0-9a-f]{64}$'
               AND p4_02_input_plan_sha256 ~ '^[0-9a-f]{64}$'
               AND prior_outcome_lineage_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT phase1a_p4_pair_batches_status_valid
        CHECK (status IN ('PREPARED', 'RELEASED', 'FAILED')),
    CONSTRAINT phase1a_p4_pair_batches_terminal_shape
        CHECK ((status = 'PREPARED'
                AND finished_at IS NULL AND error_message IS NULL)
               OR (status = 'RELEASED'
                   AND finished_at IS NOT NULL AND error_message IS NULL)
               OR (status = 'FAILED'
                   AND finished_at IS NOT NULL AND error_message IS NOT NULL)),
    CONSTRAINT phase1a_p4_pair_batches_prior_object
        CHECK (jsonb_typeof(prior_outcome_lineage) = 'object')
);

CREATE UNIQUE INDEX phase1a_p4_pair_batches_one_live_or_released
    ON systematic_fx.phase1a_p4_outcome_pair_batches (pair_id)
    WHERE status IN ('PREPARED', 'RELEASED');

CREATE TABLE systematic_fx.phase1a_p4_outcome_pair_releases (
    p4_pair_release_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    p4_pair_batch_id bigint NOT NULL UNIQUE,
    pair_id text NOT NULL UNIQUE,
    p4_01_outcome_replay_manifest_id bigint NOT NULL UNIQUE,
    p4_02_outcome_replay_manifest_id bigint NOT NULL UNIQUE,
    p4_01_run_fingerprint text NOT NULL,
    p4_02_run_fingerprint text NOT NULL,
    p4_01_result_artifact_sha256 text NOT NULL,
    p4_02_result_artifact_sha256 text NOT NULL,
    p4_01_cell_summaries_sha256 text NOT NULL,
    p4_02_cell_summaries_sha256 text NOT NULL,
    decision_sha256s jsonb NOT NULL,
    pair_config_sha256 text NOT NULL,
    prior_outcome_lineage_sha256 text NOT NULL,
    pair_economic_cell_count integer NOT NULL,
    cumulative_economic_cell_count integer NOT NULL,
    canonical_release_json text NOT NULL,
    pair_release_sha256 text NOT NULL UNIQUE,
    released_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT phase1a_p4_pair_releases_batch_fk
        FOREIGN KEY (p4_pair_batch_id)
        REFERENCES systematic_fx.phase1a_p4_outcome_pair_batches(p4_pair_batch_id),
    CONSTRAINT phase1a_p4_pair_releases_p4_01_fk
        FOREIGN KEY (p4_01_outcome_replay_manifest_id)
        REFERENCES systematic_fx.phase1a_outcome_replay_manifests
            (outcome_replay_manifest_id),
    CONSTRAINT phase1a_p4_pair_releases_p4_02_fk
        FOREIGN KEY (p4_02_outcome_replay_manifest_id)
        REFERENCES systematic_fx.phase1a_outcome_replay_manifests
            (outcome_replay_manifest_id),
    CONSTRAINT phase1a_p4_pair_releases_hashes_valid
        CHECK (p4_01_run_fingerprint ~ '^[0-9a-f]{64}$'
               AND p4_02_run_fingerprint ~ '^[0-9a-f]{64}$'
               AND p4_01_result_artifact_sha256 ~ '^[0-9a-f]{64}$'
               AND p4_02_result_artifact_sha256 ~ '^[0-9a-f]{64}$'
               AND p4_01_cell_summaries_sha256 ~ '^[0-9a-f]{64}$'
               AND p4_02_cell_summaries_sha256 ~ '^[0-9a-f]{64}$'
               AND pair_config_sha256 ~ '^[0-9a-f]{64}$'
               AND prior_outcome_lineage_sha256 ~ '^[0-9a-f]{64}$'
               AND pair_release_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT phase1a_p4_pair_releases_counts_frozen
        CHECK (pair_economic_cell_count = 1936
               AND cumulative_economic_cell_count = 3872),
    CONSTRAINT phase1a_p4_pair_releases_decisions_object
        CHECK (jsonb_typeof(decision_sha256s) = 'object')
);

CREATE FUNCTION systematic_fx.protect_phase1a_p4_outcome_pair_batch()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    p4_01 record;
    p4_02 record;
    release_exists boolean;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Phase 1A P4 pair batches are append-preserved';
    END IF;
    IF TG_OP = 'INSERT' AND NEW.status <> 'PREPARED' THEN
        RAISE EXCEPTION 'new Phase 1A P4 pair batches must begin PREPARED';
    END IF;
    IF TG_OP = 'INSERT' THEN
        SELECT EXISTS (
            SELECT 1
            FROM systematic_fx.phase1a_p4_outcome_pair_releases AS release
            WHERE release.pair_id = NEW.pair_id
        ) INTO release_exists;
        IF release_exists THEN
            RAISE EXCEPTION 'the singleton Phase 1A P4 pair is already released';
        END IF;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF OLD.status IN ('RELEASED', 'FAILED') THEN
            RAISE EXCEPTION 'terminal Phase 1A P4 pair batches are immutable';
        END IF;
        IF NEW.p4_pair_batch_id IS DISTINCT FROM OLD.p4_pair_batch_id
           OR NEW.pair_id IS DISTINCT FROM OLD.pair_id
           OR NEW.p4_01_outcome_replay_manifest_id IS DISTINCT FROM
                OLD.p4_01_outcome_replay_manifest_id
           OR NEW.p4_02_outcome_replay_manifest_id IS DISTINCT FROM
                OLD.p4_02_outcome_replay_manifest_id
           OR NEW.p4_01_run_fingerprint IS DISTINCT FROM OLD.p4_01_run_fingerprint
           OR NEW.p4_02_run_fingerprint IS DISTINCT FROM OLD.p4_02_run_fingerprint
           OR NEW.pair_config_sha256 IS DISTINCT FROM OLD.pair_config_sha256
           OR NEW.p4_01_outcome_config_sha256 IS DISTINCT FROM
                OLD.p4_01_outcome_config_sha256
           OR NEW.p4_02_outcome_config_sha256 IS DISTINCT FROM
                OLD.p4_02_outcome_config_sha256
           OR NEW.p4_01_query_definition_sha256 IS DISTINCT FROM
                OLD.p4_01_query_definition_sha256
           OR NEW.p4_02_query_definition_sha256 IS DISTINCT FROM
                OLD.p4_02_query_definition_sha256
           OR NEW.p4_01_signal_manifest_sha256 IS DISTINCT FROM
                OLD.p4_01_signal_manifest_sha256
           OR NEW.p4_02_signal_manifest_sha256 IS DISTINCT FROM
                OLD.p4_02_signal_manifest_sha256
           OR NEW.p4_01_input_plan_sha256 IS DISTINCT FROM
                OLD.p4_01_input_plan_sha256
           OR NEW.p4_02_input_plan_sha256 IS DISTINCT FROM
                OLD.p4_02_input_plan_sha256
           OR NEW.prior_outcome_lineage IS DISTINCT FROM OLD.prior_outcome_lineage
           OR NEW.prior_outcome_lineage_sha256 IS DISTINCT FROM
                OLD.prior_outcome_lineage_sha256
           OR NEW.created_at IS DISTINCT FROM OLD.created_at
           OR NEW.status NOT IN ('RELEASED', 'FAILED') THEN
            RAISE EXCEPTION 'invalid Phase 1A P4 pair batch transition';
        END IF;
    END IF;

    IF NEW.pair_id IS DISTINCT FROM 'phase1a_p4_liquidity_transition_pair_v1'
       OR NEW.pair_config_sha256 IS DISTINCT FROM
            'd83f28fae463643fc8969f8944b41c8b87254362fe709344afb7cfd240b8ea5f'
       OR NEW.p4_01_outcome_config_sha256 IS DISTINCT FROM
            'a98f0c7bcaaca70bbcfe4da7f80414a96bd664c36e025176f0163a9c2a455d25'
       OR NEW.p4_02_outcome_config_sha256 IS DISTINCT FROM
            'e9b49a0f45f4988403163085d3e4cc2e960c91cf630ea6d2cc24b7ce95a64220'
       OR NEW.p4_01_query_definition_sha256 IS DISTINCT FROM
            '39df10c27e6fa4c5070d16cb30b4c8085fe7774a36833c141d159284f7f3dc3e'
       OR NEW.p4_02_query_definition_sha256 IS DISTINCT FROM
            '825b46856dde86f7dc75393457a71d920e1eeda896f35dcd4fd47eb5fab10207'
       OR NEW.p4_01_signal_manifest_sha256 IS DISTINCT FROM
            'ef89f2dcc1a42176e4570a2b63c5d554c9e0d6fa1da77256dae3907a62a3bb59'
       OR NEW.p4_02_signal_manifest_sha256 IS DISTINCT FROM
            'c4babe44c322d391fabd305ca28b0a3274136ff611c98e2fe962b44d3d5043f4'
       OR NEW.p4_01_input_plan_sha256 IS DISTINCT FROM
            '7014967ae8aa63842ea17d0a12ff005b2656f540974af6ead8ec763f7ff73ba6'
       OR NEW.p4_02_input_plan_sha256 IS DISTINCT FROM
            '9b764e5dae1670f365046a21b0c1c5de563462fd69b2f2c91b3d7cbd547afe9c'
       OR NEW.prior_outcome_lineage_sha256 IS DISTINCT FROM
            'f56298bd8f649bfdf7b5b5432beac34968cf0f1b15f007b54803cb5d227ad6d0'
       OR NEW.prior_outcome_lineage IS DISTINCT FROM
            '{"p1_05":{"cell_summaries_sha256":"b781d6111bc098fcd846edde3e0a4378ccbefb4edbb34c5e9dae0d5be2dc65be","decision_sha256s":{"LONG":"6f2690b619cb038a174b395e830317c3a30c93d01d4f359931f8a7e9abeb1cfe","SHORT":"08215d7dd1d902a45dac82eb44de19f2caaa69b17c96f2e7e64a9d4ae99e50e8"},"detail_shard_manifest_sha256":"aca496bacc9606def65c79350a8ca3dbc76f2700d274cdc2badba097fb1fb386","final_checkpoint_sha256":"ede238cf6c45287294cc1dce2927f63dd7d2d8a78dda76f5ff59ec1c102a96de","input_lineage_sha256":"de733b7025eb0c7903fc24679f4adbd8cd859217bf1c68505e1032de75287a00","outcome_replay_manifest_id":4,"research_run_attempt_id":1305,"research_run_spec_id":1306,"result_artifact_sha256":"0bd8f465bb3bb47a7f9f72662f905a19a416802a5d8ebff23cdeefd66fcc10ce","run_fingerprint":"40730e618651c613be15d303054898757a14f1a9671be6bde7567cc921c7e97e"},"p5":{"cell_summaries_sha256":"43d8d00d1e6b32b7658df50d1f310da7dd77225bb2585aee893d9ba6be318c0e","decision_sha256s":{"LONG":"1d070437dc62115349fcc5b5e2b53f1240d6e92f681487bd4d29903f6e0ad36d","SHORT":"af1d58b4348ffa5c928027e461f58298928b899bbfb11e6e9c855876e70862e4"},"detail_shard_manifest_sha256":"79833d95c5d5ba9596e193f78d90f32a3bb13fb7b4480c752abe0a1834900af7","final_checkpoint_sha256":"1693c5e2309608f4c73505975d84d6c3117530280b12ba44e5bcaac1225a5ab7","input_lineage_sha256":"5ccd46db1cd5abc07ba2c94fca7283c5d16edc712ef64804e43eba5724433e45","outcome_replay_manifest_id":1,"research_run_attempt_id":1300,"research_run_spec_id":1300,"result_artifact_sha256":"ca9f4496c7e7e0102cf40631be060c723c16e16cccf0ef6c78986db35572fd79","run_fingerprint":"2dafdf8abfbdbcaf669f43f61443746104cb31524377a74a09964bb74768d64f"},"p5_equivalence_audit":{"audit_artifact_sha256":"b878bdfcd65a481f0710a5be5af5e4c77392260392c164ccd86db1cde6f1d309","outcome_equivalence_audit_id":1,"validation_research_run_attempt_id":1302,"validation_research_run_spec_id":1303,"validation_run_fingerprint":"b6a227c2f9c768e3b2a32c8bd7a5e2d210e7b3b053d4213b2d01055f6414ab69"}}'::jsonb THEN
        RAISE EXCEPTION 'Phase 1A P4 pair frozen lineage drift';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
        JOIN systematic_fx.research_run_attempts AS attempt
          ON attempt.research_run_attempt_id = manifest.research_run_attempt_id
         AND attempt.research_run_spec_id = manifest.research_run_spec_id
        WHERE manifest.outcome_replay_manifest_id = 1
          AND manifest.research_run_spec_id = 1300
          AND manifest.research_run_attempt_id = 1300
          AND manifest.pattern_key = 'p5_01_range_expansion_flow_continuation'
          AND manifest.status = 'SUCCEEDED'
          AND manifest.run_fingerprint =
              '2dafdf8abfbdbcaf669f43f61443746104cb31524377a74a09964bb74768d64f'
          AND manifest.result_artifact_sha256 =
              'ca9f4496c7e7e0102cf40631be060c723c16e16cccf0ef6c78986db35572fd79'
          AND manifest.cell_summaries_sha256 =
              '43d8d00d1e6b32b7658df50d1f310da7dd77225bb2585aee893d9ba6be318c0e'
          AND attempt.result_summary #>> '{input_lineage_sha256}' =
              '5ccd46db1cd5abc07ba2c94fca7283c5d16edc712ef64804e43eba5724433e45'
          AND attempt.result_summary #>> '{detail_shard_manifest_sha256}' =
              '79833d95c5d5ba9596e193f78d90f32a3bb13fb7b4480c752abe0a1834900af7'
          AND attempt.result_summary #>> '{final_checkpoint_sha256}' =
              '1693c5e2309608f4c73505975d84d6c3117530280b12ba44e5bcaac1225a5ab7'
          AND EXISTS (
              SELECT 1 FROM systematic_fx.phase1a_outcome_screening_decisions
              WHERE outcome_replay_manifest_id = 1 AND direction = 'LONG'
                AND decision_sha256 =
                    '1d070437dc62115349fcc5b5e2b53f1240d6e92f681487bd4d29903f6e0ad36d')
          AND EXISTS (
              SELECT 1 FROM systematic_fx.phase1a_outcome_screening_decisions
              WHERE outcome_replay_manifest_id = 1 AND direction = 'SHORT'
                AND decision_sha256 =
                    'af1d58b4348ffa5c928027e461f58298928b899bbfb11e6e9c855876e70862e4')
    ) OR NOT EXISTS (
        SELECT 1
        FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
        JOIN systematic_fx.research_run_attempts AS attempt
          ON attempt.research_run_attempt_id = manifest.research_run_attempt_id
         AND attempt.research_run_spec_id = manifest.research_run_spec_id
        WHERE manifest.outcome_replay_manifest_id = 4
          AND manifest.research_run_spec_id = 1306
          AND manifest.research_run_attempt_id = 1305
          AND manifest.pattern_key = 'p1_05_unconfirmed_move_reversal'
          AND manifest.status = 'SUCCEEDED'
          AND manifest.run_fingerprint =
              '40730e618651c613be15d303054898757a14f1a9671be6bde7567cc921c7e97e'
          AND manifest.result_artifact_sha256 =
              '0bd8f465bb3bb47a7f9f72662f905a19a416802a5d8ebff23cdeefd66fcc10ce'
          AND manifest.cell_summaries_sha256 =
              'b781d6111bc098fcd846edde3e0a4378ccbefb4edbb34c5e9dae0d5be2dc65be'
          AND attempt.result_summary #>> '{input_lineage_sha256}' =
              'de733b7025eb0c7903fc24679f4adbd8cd859217bf1c68505e1032de75287a00'
          AND attempt.result_summary #>> '{detail_shard_manifest_sha256}' =
              'aca496bacc9606def65c79350a8ca3dbc76f2700d274cdc2badba097fb1fb386'
          AND attempt.result_summary #>> '{final_checkpoint_sha256}' =
              'ede238cf6c45287294cc1dce2927f63dd7d2d8a78dda76f5ff59ec1c102a96de'
          AND EXISTS (
              SELECT 1 FROM systematic_fx.phase1a_outcome_screening_decisions
              WHERE outcome_replay_manifest_id = 4 AND direction = 'LONG'
                AND decision_sha256 =
                    '6f2690b619cb038a174b395e830317c3a30c93d01d4f359931f8a7e9abeb1cfe')
          AND EXISTS (
              SELECT 1 FROM systematic_fx.phase1a_outcome_screening_decisions
              WHERE outcome_replay_manifest_id = 4 AND direction = 'SHORT'
                AND decision_sha256 =
                    '08215d7dd1d902a45dac82eb44de19f2caaa69b17c96f2e7e64a9d4ae99e50e8')
    ) OR NOT EXISTS (
        SELECT 1
        FROM systematic_fx.phase1a_outcome_replay_equivalence_audits AS audit
        JOIN systematic_fx.artifacts AS artifact
          ON artifact.artifact_id = audit.audit_artifact_id
        WHERE audit.outcome_equivalence_audit_id = 1
          AND audit.validation_research_run_spec_id = 1303
          AND audit.validation_research_run_attempt_id = 1302
          AND audit.validation_run_fingerprint =
              'b6a227c2f9c768e3b2a32c8bd7a5e2d210e7b3b053d4213b2d01055f6414ab69'
          AND artifact.sha256 =
              'b878bdfcd65a481f0710a5be5af5e4c77392260392c164ccd86db1cde6f1d309'
          AND audit.passed
    ) THEN
        RAISE EXCEPTION 'Phase 1A P4 pair prior DB lineage is missing or drifted';
    END IF;

    SELECT manifest.status, manifest.pattern_key, manifest.run_fingerprint,
           spec.canonical_spec -> 'parameters' AS parameters
    INTO STRICT p4_01
    FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
    JOIN systematic_fx.research_run_specs AS spec
      ON spec.research_run_spec_id = manifest.research_run_spec_id
    WHERE manifest.outcome_replay_manifest_id =
          NEW.p4_01_outcome_replay_manifest_id;
    SELECT manifest.status, manifest.pattern_key, manifest.run_fingerprint,
           spec.canonical_spec -> 'parameters' AS parameters
    INTO STRICT p4_02
    FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
    JOIN systematic_fx.research_run_specs AS spec
      ON spec.research_run_spec_id = manifest.research_run_spec_id
    WHERE manifest.outcome_replay_manifest_id =
          NEW.p4_02_outcome_replay_manifest_id;
    IF p4_01.pattern_key IS DISTINCT FROM
            'p4_01_opposite_depth_depletion_continuation'
       OR p4_02.pattern_key IS DISTINCT FROM 'p4_02_depth_resistance_reversal'
       OR p4_01.run_fingerprint IS DISTINCT FROM NEW.p4_01_run_fingerprint
       OR p4_02.run_fingerprint IS DISTINCT FROM NEW.p4_02_run_fingerprint
       OR p4_01.parameters #>> '{pair_id}' IS DISTINCT FROM NEW.pair_id
       OR p4_02.parameters #>> '{pair_id}' IS DISTINCT FROM NEW.pair_id
       OR p4_01.parameters #>> '{pair_config_sha256}' IS DISTINCT FROM
            NEW.pair_config_sha256
       OR p4_02.parameters #>> '{pair_config_sha256}' IS DISTINCT FROM
            NEW.pair_config_sha256
       OR p4_01.parameters #>> '{outcome_config_sha256}' IS DISTINCT FROM
            NEW.p4_01_outcome_config_sha256
       OR p4_02.parameters #>> '{outcome_config_sha256}' IS DISTINCT FROM
            NEW.p4_02_outcome_config_sha256
       OR p4_01.parameters #>> '{query_definition_sha256}' IS DISTINCT FROM
            NEW.p4_01_query_definition_sha256
       OR p4_02.parameters #>> '{query_definition_sha256}' IS DISTINCT FROM
            NEW.p4_02_query_definition_sha256
       OR p4_01.parameters #>> '{signal_manifest_sha256}' IS DISTINCT FROM
            NEW.p4_01_signal_manifest_sha256
       OR p4_02.parameters #>> '{signal_manifest_sha256}' IS DISTINCT FROM
            NEW.p4_02_signal_manifest_sha256
       OR p4_01.parameters #>> '{input_plan_sha256}' IS DISTINCT FROM
            NEW.p4_01_input_plan_sha256
       OR p4_02.parameters #>> '{input_plan_sha256}' IS DISTINCT FROM
            NEW.p4_02_input_plan_sha256
       OR p4_01.parameters #>> '{prior_outcome_lineage_sha256}' IS DISTINCT FROM
            NEW.prior_outcome_lineage_sha256
       OR p4_02.parameters #>> '{prior_outcome_lineage_sha256}' IS DISTINCT FROM
            NEW.prior_outcome_lineage_sha256
       OR p4_01.parameters #> '{paired_query_ids}' IS DISTINCT FROM
            '["p4_01_opposite_depth_depletion_continuation","p4_02_depth_resistance_reversal"]'::jsonb
       OR p4_02.parameters #> '{paired_query_ids}' IS DISTINCT FROM
            '["p4_01_opposite_depth_depletion_continuation","p4_02_depth_resistance_reversal"]'::jsonb
       OR p4_01.parameters #>> '{pair_economic_cell_count}' IS DISTINCT FROM '1936'
       OR p4_02.parameters #>> '{pair_economic_cell_count}' IS DISTINCT FROM '1936'
       OR p4_01.parameters #>> '{cumulative_economic_cell_count}' IS DISTINCT FROM '3872'
       OR p4_02.parameters #>> '{cumulative_economic_cell_count}' IS DISTINCT FROM '3872' THEN
        RAISE EXCEPTION 'Phase 1A P4 pair member RunSpec lineage drift';
    END IF;

    IF TG_OP = 'INSERT' AND (p4_01.status <> 'QUEUED' OR p4_02.status <> 'QUEUED') THEN
        RAISE EXCEPTION 'new Phase 1A P4 pair batch requires two QUEUED members';
    ELSIF TG_OP = 'UPDATE' AND NEW.status = 'RELEASED' THEN
        SELECT EXISTS (
            SELECT 1 FROM systematic_fx.phase1a_p4_outcome_pair_releases
            WHERE p4_pair_batch_id = NEW.p4_pair_batch_id
        ) INTO release_exists;
        IF NOT release_exists OR p4_01.status <> 'SUCCEEDED'
           OR p4_02.status <> 'SUCCEEDED' THEN
            RAISE EXCEPTION 'P4 pair RELEASED transition requires both successes and release';
        END IF;
    ELSIF TG_OP = 'UPDATE' AND NEW.status = 'FAILED'
          AND (p4_01.status <> 'FAILED' OR p4_02.status <> 'FAILED') THEN
        RAISE EXCEPTION 'P4 pair FAILED transition requires both member failures';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER phase1a_p4_outcome_pair_batches_preserve
BEFORE INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_p4_outcome_pair_batches
FOR EACH ROW EXECUTE FUNCTION
    systematic_fx.protect_phase1a_p4_outcome_pair_batch();

CREATE FUNCTION systematic_fx.protect_phase1a_p4_outcome_manifest()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    spec record;
    parameters jsonb;
    attempt record;
    artifact record;
    expected_config_id text;
    expected_config_sha256 text;
    expected_query_definition_sha256 text;
    expected_signal_manifest_sha256 text;
    expected_input_plan_sha256 text;
    expected_occurrences integer;
    expected_detail_count integer;
    expected_planned_count integer;
    expected_long_count integer;
    expected_short_count integer;
    observed_cells integer;
    pair_batch_exists boolean;
BEGIN
    IF TG_OP = 'INSERT' AND NEW.status <> 'QUEUED' THEN
        RAISE EXCEPTION 'new Phase 1A P4 manifests must begin QUEUED';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF OLD.status IN ('SUCCEEDED', 'FAILED') THEN
            RAISE EXCEPTION 'terminal Phase 1A P4 manifests are immutable';
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
            RAISE EXCEPTION 'Phase 1A P4 manifest identity is immutable';
        END IF;
        IF NOT ((OLD.status = 'QUEUED' AND NEW.status IN ('RUNNING', 'FAILED'))
                OR (OLD.status = 'RUNNING' AND NEW.status IN ('SUCCEEDED', 'FAILED'))) THEN
            RAISE EXCEPTION 'invalid Phase 1A P4 manifest status transition';
        END IF;
    END IF;

    IF NEW.pattern_key = 'p4_01_opposite_depth_depletion_continuation' THEN
        expected_config_id := 'phase1a_p4_01_outcome_replay_v1';
        expected_config_sha256 :=
            'a98f0c7bcaaca70bbcfe4da7f80414a96bd664c36e025176f0163a9c2a455d25';
        expected_query_definition_sha256 :=
            '39df10c27e6fa4c5070d16cb30b4c8085fe7774a36833c141d159284f7f3dc3e';
        expected_signal_manifest_sha256 :=
            'ef89f2dcc1a42176e4570a2b63c5d554c9e0d6fa1da77256dae3907a62a3bb59';
        expected_input_plan_sha256 :=
            '7014967ae8aa63842ea17d0a12ff005b2656f540974af6ead8ec763f7ff73ba6';
        expected_occurrences := 334;
        expected_detail_count := 484968;
        expected_planned_count := 472;
        expected_long_count := 175;
        expected_short_count := 159;
    ELSIF NEW.pattern_key = 'p4_02_depth_resistance_reversal' THEN
        expected_config_id := 'phase1a_p4_02_outcome_replay_v1';
        expected_config_sha256 :=
            'e9b49a0f45f4988403163085d3e4cc2e960c91cf630ea6d2cc24b7ce95a64220';
        expected_query_definition_sha256 :=
            '825b46856dde86f7dc75393457a71d920e1eeda896f35dcd4fd47eb5fab10207';
        expected_signal_manifest_sha256 :=
            'c4babe44c322d391fabd305ca28b0a3274136ff611c98e2fe962b44d3d5043f4';
        expected_input_plan_sha256 :=
            '9b764e5dae1670f365046a21b0c1c5de563462fd69b2f2c91b3d7cbd547afe9c';
        expected_occurrences := 340;
        expected_detail_count := 493680;
        expected_planned_count := 455;
        expected_long_count := 159;
        expected_short_count := 181;
    ELSE
        RAISE EXCEPTION 'unknown Phase 1A P4 outcome candidate';
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
    parameters := spec.parameters;
    IF spec.campaign_key IS DISTINCT FROM 'phase1a_conservative_screening_v1'
       OR spec.run_kind IS DISTINCT FROM 'OUTCOME_BUILD'
       OR spec.engine_version IS DISTINCT FROM 'phase1a_shared_outcome_replay_v1'
       OR spec.direction IS DISTINCT FROM 'BOTH'
       OR spec.experiment_id IS NOT NULL
       OR parameters #>> '{query_id}' IS DISTINCT FROM NEW.pattern_key
       OR parameters #>> '{outcome_config_id}' IS DISTINCT FROM expected_config_id
       OR parameters #>> '{outcome_config_sha256}' IS DISTINCT FROM
            expected_config_sha256
       OR parameters #>> '{query_definition_sha256}' IS DISTINCT FROM
            expected_query_definition_sha256
       OR parameters #>> '{signal_manifest_sha256}' IS DISTINCT FROM
            expected_signal_manifest_sha256
       OR parameters #>> '{input_plan_sha256}' IS DISTINCT FROM
            expected_input_plan_sha256
       OR parameters #>> '{source_artifact_manifest_sha256}' IS DISTINCT FROM
            NEW.source_artifact_manifest_sha256
       OR parameters #>> '{source_slice_count}' IS DISTINCT FROM '99'
       OR parameters #>> '{source_occurrence_count}' IS DISTINCT FROM
            expected_occurrences::text
       OR parameters #>> '{expected_detail_record_count}' IS DISTINCT FROM
            expected_detail_count::text
       OR parameters #>> '{planned_source_date_count}' IS DISTINCT FROM
            expected_planned_count::text
       OR parameters #>> '{final_source_date}' IS DISTINCT FROM '2023-08-31'
       OR parameters #> '{expected_direction_signal_counts}' IS DISTINCT FROM
            jsonb_build_object('LONG', expected_long_count,
                               'SHORT', expected_short_count)
       OR parameters #>> '{pair_id}' IS DISTINCT FROM
            'phase1a_p4_liquidity_transition_pair_v1'
       OR parameters #>> '{pair_config_sha256}' IS DISTINCT FROM
            'd83f28fae463643fc8969f8944b41c8b87254362fe709344afb7cfd240b8ea5f'
       OR parameters #>> '{prior_outcome_lineage_sha256}' IS DISTINCT FROM
            'f56298bd8f649bfdf7b5b5432beac34968cf0f1b15f007b54803cb5d227ad6d0'
       OR parameters #> '{paired_query_ids}' IS DISTINCT FROM
            '["p4_01_opposite_depth_depletion_continuation","p4_02_depth_resistance_reversal"]'::jsonb
       OR parameters #>> '{pair_economic_cell_count}' IS DISTINCT FROM '1936'
       OR parameters #>> '{cumulative_economic_cell_count}' IS DISTINCT FROM '3872'
       OR parameters #>> '{cell_count_per_surface}' IS DISTINCT FROM '484'
       OR parameters #>> '{expected_summary_count}' IS DISTINCT FROM '2904'
       OR parameters #> '{scenario_ids}' IS DISTINCT FROM
            '["BASELINE","MODERATE_COMBINED","SEVERE_DIAGNOSTIC"]'::jsonb
       OR parameters #> '{direction_ids}' IS DISTINCT FROM '["LONG","SHORT"]'::jsonb
       OR parameters #> '{take_profit_ticks}' IS DISTINCT FROM
            '[24,32,40,48,56,64,72,80,88,96,104,112,120,128,136,144,152,160,168,176,184,192]'::jsonb
       OR parameters #> '{stop_loss_ticks}' IS DISTINCT FROM
            '[24,32,40,48,56,64,72,80,88,96,104,112,120,128,136,144,152,160,168,176,184,192]'::jsonb
       OR parameters #> '{scenario_cost_ticks_per_fill}' IS DISTINCT FROM
            '{"BASELINE":{"allocated_fixed":4,"variable":4},"MODERATE_COMBINED":{"allocated_fixed":5,"variable":5},"SEVERE_DIAGNOSTIC":{"allocated_fixed":6,"variable":6}}'::jsonb THEN
        RAISE EXCEPTION 'Phase 1A P4 outcome RunSpec parameter drift';
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
        RAISE EXCEPTION 'P4 outcome manifest and generic attempt state differ';
    END IF;

    IF TG_OP = 'UPDATE' AND NEW.status = 'RUNNING' THEN
        SELECT EXISTS (
            SELECT 1
            FROM systematic_fx.phase1a_p4_outcome_pair_batches AS batch
            WHERE batch.status = 'PREPARED'
              AND ((NEW.pattern_key =
                    'p4_01_opposite_depth_depletion_continuation'
                    AND batch.p4_01_outcome_replay_manifest_id =
                        NEW.outcome_replay_manifest_id
                    AND batch.p4_01_run_fingerprint = NEW.run_fingerprint)
                   OR (NEW.pattern_key = 'p4_02_depth_resistance_reversal'
                       AND batch.p4_02_outcome_replay_manifest_id =
                           NEW.outcome_replay_manifest_id
                       AND batch.p4_02_run_fingerprint = NEW.run_fingerprint))
        ) INTO pair_batch_exists;
        IF NOT pair_batch_exists THEN
            RAISE EXCEPTION 'P4 replay requires its exact PREPARED pair before RUNNING';
        END IF;
    END IF;

    IF NEW.status = 'SUCCEEDED' THEN
        SELECT artifact_type, uri, sha256, byte_size, metadata
        INTO STRICT artifact
        FROM systematic_fx.artifacts
        WHERE artifact_id = NEW.result_artifact_id;
        IF artifact.artifact_type IS DISTINCT FROM
                'PHASE1A_OUTCOME_REPLAY_RESULT'
           OR position('/data/derived/' IN artifact.uri) = 0
           OR artifact.sha256 IS DISTINCT FROM NEW.result_artifact_sha256
           OR artifact.byte_size IS DISTINCT FROM NEW.result_artifact_byte_size
           OR artifact.metadata #>> '{campaign_key}' IS DISTINCT FROM
                'phase1a_conservative_screening_v1'
           OR artifact.metadata #>> '{query_id}' IS DISTINCT FROM NEW.pattern_key
           OR artifact.metadata #>> '{outcome_config_id}' IS DISTINCT FROM
                expected_config_id
           OR artifact.metadata #>> '{pair_id}' IS DISTINCT FROM
                'phase1a_p4_liquidity_transition_pair_v1'
           OR artifact.metadata #>> '{pair_config_sha256}' IS DISTINCT FROM
                'd83f28fae463643fc8969f8944b41c8b87254362fe709344afb7cfd240b8ea5f'
           OR artifact.metadata #>> '{prior_outcome_lineage_sha256}' IS DISTINCT FROM
                'f56298bd8f649bfdf7b5b5432beac34968cf0f1b15f007b54803cb5d227ad6d0'
           OR artifact.metadata #> '{paired_query_ids}' IS DISTINCT FROM
                '["p4_01_opposite_depth_depletion_continuation","p4_02_depth_resistance_reversal"]'::jsonb
           OR artifact.metadata #>> '{pair_economic_cell_count}' IS DISTINCT FROM '1936'
           OR artifact.metadata #>> '{cumulative_economic_cell_count}' IS DISTINCT FROM '3872'
           OR artifact.metadata #>> '{run_fingerprint}' IS DISTINCT FROM
                NEW.run_fingerprint
           OR artifact.metadata #>> '{source_artifact_manifest_sha256}' IS DISTINCT FROM
                NEW.source_artifact_manifest_sha256
           OR artifact.metadata #>> '{cell_summaries_sha256}' IS DISTINCT FROM
                NEW.cell_summaries_sha256
           OR artifact.metadata #>> '{summary_row_count}' IS DISTINCT FROM '2904'
           OR artifact.metadata #>> '{detail_record_count}' IS DISTINCT FROM
                expected_detail_count::text
           OR artifact.metadata #>> '{planned_source_date_count}' IS DISTINCT FROM
                expected_planned_count::text THEN
            RAISE EXCEPTION 'Phase 1A P4 result artifact lineage drift';
        END IF;
        SELECT count(*)::integer INTO observed_cells
        FROM systematic_fx.phase1a_outcome_cell_summaries
        WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
          AND run_fingerprint = NEW.run_fingerprint;
        IF observed_cells <> 2904 THEN
            RAISE EXCEPTION 'Phase 1A P4 success requires all 2904 summaries';
        END IF;
        IF attempt.result_summary #>> '{artifact_sha256}' IS DISTINCT FROM
                NEW.result_artifact_sha256
           OR attempt.result_summary #>> '{cell_summaries_sha256}' IS DISTINCT FROM
                NEW.cell_summaries_sha256
           OR attempt.result_summary #>> '{query_id}' IS DISTINCT FROM NEW.pattern_key
           OR attempt.result_summary #>> '{pair_id}' IS DISTINCT FROM
                'phase1a_p4_liquidity_transition_pair_v1'
           OR attempt.result_summary #>> '{pair_config_sha256}' IS DISTINCT FROM
                'd83f28fae463643fc8969f8944b41c8b87254362fe709344afb7cfd240b8ea5f'
           OR attempt.result_summary #>> '{prior_outcome_lineage_sha256}' IS DISTINCT FROM
                'f56298bd8f649bfdf7b5b5432beac34968cf0f1b15f007b54803cb5d227ad6d0'
           OR attempt.result_summary #> '{paired_query_ids}' IS DISTINCT FROM
                '["p4_01_opposite_depth_depletion_continuation","p4_02_depth_resistance_reversal"]'::jsonb
           OR attempt.result_summary #>> '{pair_economic_cell_count}' IS DISTINCT FROM '1936'
           OR attempt.result_summary #>> '{cumulative_economic_cell_count}' IS DISTINCT FROM '3872'
           OR attempt.result_summary #>> '{summary_row_count}' IS DISTINCT FROM '2904'
           OR attempt.result_summary #>> '{detail_record_count}' IS DISTINCT FROM
                expected_detail_count::text
           OR attempt.result_summary #>> '{planned_source_date_count}' IS DISTINCT FROM
                expected_planned_count::text THEN
            RAISE EXCEPTION 'Phase 1A P4 attempt result summary drift';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER phase1a_p4_outcome_manifests_preserve_and_validate
BEFORE INSERT OR UPDATE
ON systematic_fx.phase1a_outcome_replay_manifests
FOR EACH ROW
WHEN (NEW.pattern_key IN
      ('p4_01_opposite_depth_depletion_continuation',
       'p4_02_depth_resistance_reversal'))
EXECUTE FUNCTION systematic_fx.protect_phase1a_p4_outcome_manifest();

CREATE FUNCTION systematic_fx.harden_phase1a_p4_outcome_completion()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    parameters jsonb;
    artifact_metadata jsonb;
    attempt_summary jsonb;
    expected_detail_count integer;
    expected_planned_count integer;
    expected_long_count integer;
    expected_short_count integer;
    observed_checkpoint_count integer;
    final_checkpoint record;
BEGIN
    IF NEW.pattern_key = 'p4_01_opposite_depth_depletion_continuation' THEN
        expected_detail_count := 484968;
        expected_planned_count := 472;
        expected_long_count := 175;
        expected_short_count := 159;
    ELSIF NEW.pattern_key = 'p4_02_depth_resistance_reversal' THEN
        expected_detail_count := 493680;
        expected_planned_count := 455;
        expected_long_count := 159;
        expected_short_count := 181;
    ELSE
        RAISE EXCEPTION 'unknown Phase 1A P4 completion candidate';
    END IF;
    SELECT canonical_spec -> 'parameters' INTO STRICT parameters
    FROM systematic_fx.research_run_specs
    WHERE research_run_spec_id = NEW.research_run_spec_id
      AND campaign_id = NEW.campaign_id
      AND run_fingerprint = NEW.run_fingerprint;
    IF parameters #>> '{expected_completed_source_date_count}' IS DISTINCT FROM
            expected_planned_count::text
       OR parameters #>> '{expected_last_completed_source_date}' IS DISTINCT FROM
            '2023-08-31'
       OR parameters #>> '{expected_detail_record_count}' IS DISTINCT FROM
            expected_detail_count::text
       OR parameters #> '{expected_direction_signal_counts}' IS DISTINCT FROM
            jsonb_build_object('LONG', expected_long_count,
                               'SHORT', expected_short_count)
       OR parameters #> '{scenario_cost_ticks_per_fill}' IS DISTINCT FROM
            '{"BASELINE":{"allocated_fixed":4,"variable":4},"MODERATE_COMBINED":{"allocated_fixed":5,"variable":5},"SEVERE_DIAGNOSTIC":{"allocated_fixed":6,"variable":6}}'::jsonb THEN
        RAISE EXCEPTION 'Phase 1A P4 completion parameter drift';
    END IF;
    IF NEW.status <> 'SUCCEEDED' THEN
        RETURN NEW;
    END IF;
    SELECT metadata INTO STRICT artifact_metadata
    FROM systematic_fx.artifacts
    WHERE artifact_id = NEW.result_artifact_id;
    IF artifact_metadata #>> '{detail_record_count}' IS DISTINCT FROM
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
        RAISE EXCEPTION 'Phase 1A P4 result completion lineage drift';
    END IF;
    SELECT count(*)::integer INTO observed_checkpoint_count
    FROM systematic_fx.phase1a_outcome_replay_checkpoints
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
      AND run_fingerprint = NEW.run_fingerprint;
    IF observed_checkpoint_count <> expected_planned_count THEN
        RAISE EXCEPTION 'Phase 1A P4 completion requires every source-date checkpoint';
    END IF;
    SELECT checkpoint_sequence, completed_source_date_count,
           last_completed_source_date, checkpoint_artifact_sha256,
           progress_metadata
    INTO STRICT final_checkpoint
    FROM systematic_fx.phase1a_outcome_replay_checkpoints
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
      AND run_fingerprint = NEW.run_fingerprint
    ORDER BY checkpoint_sequence DESC
    LIMIT 1;
    IF final_checkpoint.checkpoint_sequence <> expected_planned_count
       OR final_checkpoint.completed_source_date_count <> expected_planned_count
       OR final_checkpoint.last_completed_source_date <> DATE '2023-08-31'
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
       OR final_checkpoint.progress_metadata #>> '{detail_shard_manifest_sha256}'
            IS DISTINCT FROM artifact_metadata #>> '{detail_shard_manifest_sha256}'
       OR final_checkpoint.progress_metadata #>> '{input_lineage_sha256}' IS DISTINCT FROM
            artifact_metadata #>> '{input_lineage_sha256}' THEN
        RAISE EXCEPTION 'Phase 1A P4 final checkpoint is incomplete or unbound';
    END IF;
    SELECT result_summary INTO STRICT attempt_summary
    FROM systematic_fx.research_run_attempts
    WHERE research_run_attempt_id = NEW.research_run_attempt_id
      AND research_run_spec_id = NEW.research_run_spec_id;
    IF attempt_summary #>> '{cache_manifest_sha256}' IS DISTINCT FROM
            artifact_metadata #>> '{cache_manifest_sha256}'
       OR attempt_summary #>> '{detail_shard_manifest_sha256}' IS DISTINCT FROM
            artifact_metadata #>> '{detail_shard_manifest_sha256}'
       OR attempt_summary #>> '{final_checkpoint_sha256}' IS DISTINCT FROM
            artifact_metadata #>> '{final_checkpoint_sha256}'
       OR attempt_summary #>> '{input_lineage_sha256}' IS DISTINCT FROM
            artifact_metadata #>> '{input_lineage_sha256}' THEN
        RAISE EXCEPTION 'Phase 1A P4 attempt completion lineage drift';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER phase1a_p4_outcome_manifest_completion_hardening
BEFORE INSERT OR UPDATE
ON systematic_fx.phase1a_outcome_replay_manifests
FOR EACH ROW
WHEN (NEW.pattern_key IN
      ('p4_01_opposite_depth_depletion_continuation',
       'p4_02_depth_resistance_reversal'))
EXECUTE FUNCTION systematic_fx.harden_phase1a_p4_outcome_completion();

CREATE FUNCTION systematic_fx.phase1a_outcome_cell_summary_payload(
    cell systematic_fx.phase1a_outcome_cell_summaries
)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $$
    SELECT jsonb_build_object(
        'allocated_fixed_cost_ticks', ($1).allocated_fixed_cost_ticks,
        'calendar_month_net_pnl_usd', ($1).calendar_month_net_pnl_usd::text,
        'censored_count', ($1).censored_count,
        'complete', ($1).complete,
        'direction', ($1).direction,
        'entry_fill_count', ($1).entry_fill_count,
        'entry_not_filled_count', ($1).entry_not_filled_count,
        'fully_loaded_net_ev_ticks', ($1).fully_loaded_net_ev_ticks::text,
        'fully_loaded_net_pnl_ticks', ($1).fully_loaded_net_pnl_ticks,
        'fully_loaded_net_pnl_usd', ($1).fully_loaded_net_pnl_usd::text,
        'gross_pnl_ticks', ($1).gross_pnl_ticks,
        'maximum_drawdown_usd', ($1).maximum_drawdown_usd::text,
        'profit_factor', ($1).profit_factor::text,
        'scenario_id', ($1).scenario_id,
        'signal_count', ($1).signal_count,
        'skipped_occupied_count', ($1).skipped_occupied_count,
        'stop_first_count', ($1).stop_first_count,
        'stop_loss_ticks', ($1).stop_loss_ticks,
        'take_profit_first_count', ($1).take_profit_first_count,
        'take_profit_ticks', ($1).take_profit_ticks,
        'terminal_exit_count', ($1).terminal_exit_count,
        'variable_cost_ticks', ($1).variable_cost_ticks
    );
$$;

CREATE OR REPLACE FUNCTION systematic_fx.protect_phase1a_ordered_outcome_cell_summary()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    manifest record;
    expected_signal_count integer;
    expected_cost_per_fill integer;
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
        WHEN manifest.pattern_key =
             'p4_01_opposite_depth_depletion_continuation'
             AND NEW.direction = 'LONG' THEN 175
        WHEN manifest.pattern_key =
             'p4_01_opposite_depth_depletion_continuation'
             AND NEW.direction = 'SHORT' THEN 159
        WHEN manifest.pattern_key = 'p4_02_depth_resistance_reversal'
             AND NEW.direction = 'LONG' THEN 159
        WHEN manifest.pattern_key = 'p4_02_depth_resistance_reversal'
             AND NEW.direction = 'SHORT' THEN 181
        ELSE NULL
    END;
    IF expected_signal_count IS NULL OR NEW.signal_count <> expected_signal_count THEN
        RAISE EXCEPTION 'outcome cell signal_count differs from its frozen query/direction';
    END IF;
    expected_cost_per_fill := CASE NEW.scenario_id
        WHEN 'BASELINE' THEN 4
        WHEN 'MODERATE_COMBINED' THEN 5
        WHEN 'SEVERE_DIAGNOSTIC' THEN 6
        ELSE NULL
    END;
    IF manifest.pattern_key IN
       ('p4_01_opposite_depth_depletion_continuation',
        'p4_02_depth_resistance_reversal')
       AND (expected_cost_per_fill IS NULL
            OR NEW.variable_cost_ticks <>
                NEW.entry_fill_count * expected_cost_per_fill
            OR NEW.allocated_fixed_cost_ticks <>
                NEW.entry_fill_count * expected_cost_per_fill) THEN
        RAISE EXCEPTION 'Phase 1A P4 outcome cell scenario-cost accounting drift';
    END IF;
    IF manifest.pattern_key IN
       ('p4_01_opposite_depth_depletion_continuation',
        'p4_02_depth_resistance_reversal')
       AND ((NEW.fully_loaded_net_ev_ticks IS NOT NULL
             AND lower(NEW.fully_loaded_net_ev_ticks::text) IN
                 ('nan', 'infinity', '-infinity'))
            OR lower(NEW.fully_loaded_net_pnl_usd::text) IN
                 ('nan', 'infinity', '-infinity')
            OR lower(NEW.calendar_month_net_pnl_usd::text) IN
                 ('nan', 'infinity', '-infinity')
            OR (NEW.profit_factor IS NOT NULL
                AND lower(NEW.profit_factor::text) IN
                    ('nan', 'infinity', '-infinity'))
            OR lower(NEW.maximum_drawdown_usd::text) IN
                 ('nan', 'infinity', '-infinity')) THEN
        RAISE EXCEPTION 'Phase 1A P4 outcome cell decimal metrics must be finite';
    END IF;
    IF manifest.pattern_key IN
       ('p4_01_opposite_depth_depletion_continuation',
        'p4_02_depth_resistance_reversal')
       AND NEW.summary_sha256 IS DISTINCT FROM
            systematic_fx.canonical_jsonb_sha256(
                systematic_fx.phase1a_outcome_cell_summary_payload(NEW)
            ) THEN
        RAISE EXCEPTION 'Phase 1A P4 outcome cell summary SHA-256 drift';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION systematic_fx.protect_phase1a_p4_screening_decision_sha256()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    pattern_key_value text;
    expected_payload jsonb;
    reason_count integer;
    distinct_reason_count integer;
    reasons_valid boolean;
BEGIN
    SELECT pattern_key INTO STRICT pattern_key_value
    FROM systematic_fx.phase1a_outcome_replay_manifests
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id;
    IF pattern_key_value NOT IN
       ('p4_01_opposite_depth_depletion_continuation',
        'p4_02_depth_resistance_reversal') THEN
        RETURN NEW;
    END IF;
    IF jsonb_typeof(NEW.rejection_reasons) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'Phase 1A P4 screening decision reasons must be an array';
    END IF;
    SELECT count(*)::integer,
           count(DISTINCT reason.value #>> '{}')::integer,
           COALESCE(
               bool_and(jsonb_typeof(reason.value) = 'string'
                        AND btrim(reason.value #>> '{}', E' \t\n\r\f\v') <> ''),
               true
           )
    INTO reason_count, distinct_reason_count, reasons_valid
    FROM jsonb_array_elements(NEW.rejection_reasons) AS reason(value);
    IF NOT reasons_valid OR reason_count <> distinct_reason_count THEN
        RAISE EXCEPTION
            'Phase 1A P4 screening decision reasons must be unique nonblank strings';
    END IF;
    expected_payload := jsonb_build_object(
        'decision_label', NEW.decision_label,
        'direction', NEW.direction,
        'outcome_replay_manifest_id', NEW.outcome_replay_manifest_id,
        'positive_region_size', NEW.positive_region_size,
        'rejection_reasons', NEW.rejection_reasons,
        'selected_stop_loss_ticks', NEW.selected_stop_loss_ticks,
        'selected_take_profit_ticks', NEW.selected_take_profit_ticks
    );
    IF NEW.decision_sha256 IS DISTINCT FROM
            systematic_fx.canonical_jsonb_sha256(expected_payload) THEN
        RAISE EXCEPTION 'Phase 1A P4 screening decision SHA-256 drift';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER phase1a_p4_screening_decisions_validate_sha256
BEFORE INSERT
ON systematic_fx.phase1a_outcome_screening_decisions
FOR EACH ROW EXECUTE FUNCTION
    systematic_fx.protect_phase1a_p4_screening_decision_sha256();

CREATE OR REPLACE FUNCTION systematic_fx.require_phase1a_ordered_outcome_attempt_manifest()
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
               'p1_05_unconfirmed_move_reversal',
               'p4_01_opposite_depth_depletion_continuation',
               'p4_02_depth_resistance_reversal')
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

CREATE FUNCTION systematic_fx.protect_phase1a_p4_outcome_pair_release()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    batch record;
    p4_01 record;
    p4_02 record;
    observed_p4_01_cells integer;
    observed_p4_02_cells integer;
    observed_p4_01_cells_sha256 text;
    observed_p4_02_cells_sha256 text;
    observed_decisions jsonb;
    expected_payload jsonb;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Phase 1A P4 pair releases are append-only';
    END IF;
    SELECT * INTO STRICT batch
    FROM systematic_fx.phase1a_p4_outcome_pair_batches
    WHERE p4_pair_batch_id = NEW.p4_pair_batch_id;
    IF batch.status <> 'PREPARED'
       OR NEW.pair_id IS DISTINCT FROM batch.pair_id
       OR NEW.p4_01_outcome_replay_manifest_id IS DISTINCT FROM
            batch.p4_01_outcome_replay_manifest_id
       OR NEW.p4_02_outcome_replay_manifest_id IS DISTINCT FROM
            batch.p4_02_outcome_replay_manifest_id
       OR NEW.p4_01_run_fingerprint IS DISTINCT FROM batch.p4_01_run_fingerprint
       OR NEW.p4_02_run_fingerprint IS DISTINCT FROM batch.p4_02_run_fingerprint
       OR NEW.pair_config_sha256 IS DISTINCT FROM batch.pair_config_sha256
       OR NEW.prior_outcome_lineage_sha256 IS DISTINCT FROM
            batch.prior_outcome_lineage_sha256 THEN
        RAISE EXCEPTION 'P4 release differs from its PREPARED batch';
    END IF;
    SELECT status, pattern_key, run_fingerprint, result_artifact_id,
           result_artifact_sha256, cell_summaries_sha256
    INTO STRICT p4_01
    FROM systematic_fx.phase1a_outcome_replay_manifests
    WHERE outcome_replay_manifest_id = NEW.p4_01_outcome_replay_manifest_id;
    SELECT status, pattern_key, run_fingerprint, result_artifact_id,
           result_artifact_sha256, cell_summaries_sha256
    INTO STRICT p4_02
    FROM systematic_fx.phase1a_outcome_replay_manifests
    WHERE outcome_replay_manifest_id = NEW.p4_02_outcome_replay_manifest_id;
    IF p4_01.status <> 'SUCCEEDED' OR p4_02.status <> 'SUCCEEDED'
       OR p4_01.pattern_key <> 'p4_01_opposite_depth_depletion_continuation'
       OR p4_02.pattern_key <> 'p4_02_depth_resistance_reversal'
       OR p4_01.run_fingerprint IS DISTINCT FROM NEW.p4_01_run_fingerprint
       OR p4_02.run_fingerprint IS DISTINCT FROM NEW.p4_02_run_fingerprint
       OR p4_01.result_artifact_id IS NULL OR p4_02.result_artifact_id IS NULL
       OR p4_01.result_artifact_sha256 IS DISTINCT FROM
            NEW.p4_01_result_artifact_sha256
       OR p4_02.result_artifact_sha256 IS DISTINCT FROM
            NEW.p4_02_result_artifact_sha256
       OR p4_01.cell_summaries_sha256 IS DISTINCT FROM
            NEW.p4_01_cell_summaries_sha256
       OR p4_02.cell_summaries_sha256 IS DISTINCT FROM
            NEW.p4_02_cell_summaries_sha256 THEN
        RAISE EXCEPTION 'P4 release member success identity drift';
    END IF;
    SELECT count(*)::integer,
           systematic_fx.canonical_jsonb_sha256(
               jsonb_agg(
                   systematic_fx.phase1a_outcome_cell_summary_payload(cell)
                   ORDER BY
                       CASE cell.scenario_id
                           WHEN 'BASELINE' THEN 1
                           WHEN 'MODERATE_COMBINED' THEN 2
                           WHEN 'SEVERE_DIAGNOSTIC' THEN 3
                       END,
                       CASE cell.direction WHEN 'LONG' THEN 1 ELSE 2 END,
                       cell.take_profit_ticks,
                       cell.stop_loss_ticks
               )
           )
    INTO observed_p4_01_cells, observed_p4_01_cells_sha256
    FROM systematic_fx.phase1a_outcome_cell_summaries AS cell
    WHERE outcome_replay_manifest_id = NEW.p4_01_outcome_replay_manifest_id;
    SELECT count(*)::integer,
           systematic_fx.canonical_jsonb_sha256(
               jsonb_agg(
                   systematic_fx.phase1a_outcome_cell_summary_payload(cell)
                   ORDER BY
                       CASE cell.scenario_id
                           WHEN 'BASELINE' THEN 1
                           WHEN 'MODERATE_COMBINED' THEN 2
                           WHEN 'SEVERE_DIAGNOSTIC' THEN 3
                       END,
                       CASE cell.direction WHEN 'LONG' THEN 1 ELSE 2 END,
                       cell.take_profit_ticks,
                       cell.stop_loss_ticks
               )
           )
    INTO observed_p4_02_cells, observed_p4_02_cells_sha256
    FROM systematic_fx.phase1a_outcome_cell_summaries AS cell
    WHERE outcome_replay_manifest_id = NEW.p4_02_outcome_replay_manifest_id;
    IF observed_p4_01_cells <> 2904 OR observed_p4_02_cells <> 2904 THEN
        RAISE EXCEPTION 'P4 release requires exactly 5808 ordered summaries';
    END IF;
    IF observed_p4_01_cells_sha256 IS DISTINCT FROM
            NEW.p4_01_cell_summaries_sha256
       OR observed_p4_02_cells_sha256 IS DISTINCT FROM
            NEW.p4_02_cell_summaries_sha256 THEN
        RAISE EXCEPTION 'P4 release ordered cell-summary aggregate SHA-256 drift';
    END IF;
    SELECT jsonb_build_object(
        'p4_01_opposite_depth_depletion_continuation',
        jsonb_build_object(
            'LONG', max(decision_sha256)
                FILTER (WHERE outcome_replay_manifest_id =
                              NEW.p4_01_outcome_replay_manifest_id
                        AND direction = 'LONG'),
            'SHORT', max(decision_sha256)
                FILTER (WHERE outcome_replay_manifest_id =
                              NEW.p4_01_outcome_replay_manifest_id
                        AND direction = 'SHORT')),
        'p4_02_depth_resistance_reversal',
        jsonb_build_object(
            'LONG', max(decision_sha256)
                FILTER (WHERE outcome_replay_manifest_id =
                              NEW.p4_02_outcome_replay_manifest_id
                        AND direction = 'LONG'),
            'SHORT', max(decision_sha256)
                FILTER (WHERE outcome_replay_manifest_id =
                              NEW.p4_02_outcome_replay_manifest_id
                        AND direction = 'SHORT')))
    INTO observed_decisions
    FROM systematic_fx.phase1a_outcome_screening_decisions
    WHERE outcome_replay_manifest_id IN
          (NEW.p4_01_outcome_replay_manifest_id,
           NEW.p4_02_outcome_replay_manifest_id);
    IF observed_decisions IS DISTINCT FROM NEW.decision_sha256s
       OR NEW.decision_sha256s #>>
            '{p4_01_opposite_depth_depletion_continuation,LONG}' IS NULL
       OR NEW.decision_sha256s #>>
            '{p4_01_opposite_depth_depletion_continuation,SHORT}' IS NULL
       OR NEW.decision_sha256s #>>
            '{p4_02_depth_resistance_reversal,LONG}' IS NULL
       OR NEW.decision_sha256s #>>
            '{p4_02_depth_resistance_reversal,SHORT}' IS NULL THEN
        RAISE EXCEPTION 'P4 release requires the exact four screening decisions';
    END IF;

    expected_payload := jsonb_build_object(
        'cumulative_economic_cell_count', 3872,
        'decision_count', 4,
        'decision_sha256s', observed_decisions,
        'expected_candidate_count', 2,
        'expected_detail_record_count', 978648,
        'expected_signal_count', 674,
        'expected_summary_count', 5808,
        'members', jsonb_build_array(
            jsonb_build_object(
                'cell_summaries_sha256', NEW.p4_01_cell_summaries_sha256,
                'detail_record_count', 484968,
                'direction_signal_counts', jsonb_build_object('LONG', 175, 'SHORT', 159),
                'input_plan_sha256', batch.p4_01_input_plan_sha256,
                'outcome_config_id', 'phase1a_p4_01_outcome_replay_v1',
                'outcome_config_sha256', batch.p4_01_outcome_config_sha256,
                'outcome_replay_manifest_id', NEW.p4_01_outcome_replay_manifest_id,
                'planned_source_date_count', 472,
                'query_definition_sha256', batch.p4_01_query_definition_sha256,
                'query_id', 'p4_01_opposite_depth_depletion_continuation',
                'result_artifact_sha256', NEW.p4_01_result_artifact_sha256,
                'run_fingerprint', NEW.p4_01_run_fingerprint,
                'signal_manifest_sha256', batch.p4_01_signal_manifest_sha256,
                'source_occurrence_count', 334,
                'summary_count', 2904),
            jsonb_build_object(
                'cell_summaries_sha256', NEW.p4_02_cell_summaries_sha256,
                'detail_record_count', 493680,
                'direction_signal_counts', jsonb_build_object('LONG', 159, 'SHORT', 181),
                'input_plan_sha256', batch.p4_02_input_plan_sha256,
                'outcome_config_id', 'phase1a_p4_02_outcome_replay_v1',
                'outcome_config_sha256', batch.p4_02_outcome_config_sha256,
                'outcome_replay_manifest_id', NEW.p4_02_outcome_replay_manifest_id,
                'planned_source_date_count', 455,
                'query_definition_sha256', batch.p4_02_query_definition_sha256,
                'query_id', 'p4_02_depth_resistance_reversal',
                'result_artifact_sha256', NEW.p4_02_result_artifact_sha256,
                'run_fingerprint', NEW.p4_02_run_fingerprint,
                'signal_manifest_sha256', batch.p4_02_signal_manifest_sha256,
                'source_occurrence_count', 340,
                'summary_count', 2904)),
        'pair_config_sha256', batch.pair_config_sha256,
        'pair_economic_cell_count', 1936,
        'pair_id', batch.pair_id,
        'prior_outcome_lineage_sha256', batch.prior_outcome_lineage_sha256);
    IF NEW.canonical_release_json IS DISTINCT FROM
            systematic_fx.canonical_jsonb_text(expected_payload)
       OR NEW.pair_release_sha256 IS DISTINCT FROM
            systematic_fx.canonical_jsonb_sha256(expected_payload) THEN
        RAISE EXCEPTION 'P4 canonical release payload or SHA-256 drift';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER phase1a_p4_outcome_pair_releases_append_only
BEFORE INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_p4_outcome_pair_releases
FOR EACH ROW EXECUTE FUNCTION
    systematic_fx.protect_phase1a_p4_outcome_pair_release();

CREATE FUNCTION systematic_fx.require_phase1a_p4_terminal_pair()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    batch record;
    release_count integer;
    p4_01_status text;
    p4_02_status text;
BEGIN
    IF NEW.pattern_key NOT IN
       ('p4_01_opposite_depth_depletion_continuation',
        'p4_02_depth_resistance_reversal') THEN
        RETURN NULL;
    END IF;
    SELECT * INTO batch
    FROM systematic_fx.phase1a_p4_outcome_pair_batches
    WHERE p4_01_outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
       OR p4_02_outcome_replay_manifest_id = NEW.outcome_replay_manifest_id;
    IF NEW.status IN ('QUEUED', 'FAILED') AND NOT FOUND THEN
        RETURN NULL;
    END IF;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'nonfailed P4 replay must belong to one pair batch';
    END IF;
    SELECT status INTO STRICT p4_01_status
    FROM systematic_fx.phase1a_outcome_replay_manifests
    WHERE outcome_replay_manifest_id = batch.p4_01_outcome_replay_manifest_id;
    SELECT status INTO STRICT p4_02_status
    FROM systematic_fx.phase1a_outcome_replay_manifests
    WHERE outcome_replay_manifest_id = batch.p4_02_outcome_replay_manifest_id;
    IF NEW.status = 'RUNNING' AND batch.status <> 'PREPARED' THEN
        RAISE EXCEPTION 'RUNNING P4 replay requires a PREPARED pair batch';
    ELSIF NEW.status = 'FAILED'
          AND (batch.status <> 'FAILED'
               OR p4_01_status <> 'FAILED' OR p4_02_status <> 'FAILED') THEN
        RAISE EXCEPTION 'pair-bound P4 failures must terminalize together';
    ELSIF NEW.status = 'SUCCEEDED' THEN
        SELECT count(*)::integer INTO release_count
        FROM systematic_fx.phase1a_p4_outcome_pair_releases
        WHERE p4_pair_batch_id = batch.p4_pair_batch_id;
        IF batch.status <> 'RELEASED'
           OR p4_01_status <> 'SUCCEEDED' OR p4_02_status <> 'SUCCEEDED'
           OR release_count <> 1 THEN
            RAISE EXCEPTION 'successful P4 replay requires one atomic pair release';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER phase1a_p4_outcome_manifests_require_terminal_pair
AFTER INSERT OR UPDATE
ON systematic_fx.phase1a_outcome_replay_manifests
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION systematic_fx.require_phase1a_p4_terminal_pair();

CREATE FUNCTION systematic_fx.require_phase1a_p4_cell_pair_release()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    query_id text;
    released boolean;
BEGIN
    SELECT pattern_key INTO STRICT query_id
    FROM systematic_fx.phase1a_outcome_replay_manifests
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id;
    IF query_id NOT IN
       ('p4_01_opposite_depth_depletion_continuation',
        'p4_02_depth_resistance_reversal') THEN
        RETURN NULL;
    END IF;
    SELECT EXISTS (
        SELECT 1
        FROM systematic_fx.phase1a_p4_outcome_pair_releases
        WHERE p4_01_outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
           OR p4_02_outcome_replay_manifest_id = NEW.outcome_replay_manifest_id
    ) INTO released;
    IF NOT released THEN
        RAISE EXCEPTION 'P4 cell summaries cannot commit outside atomic pair release';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER phase1a_p4_outcome_cells_require_pair_release
AFTER INSERT
ON systematic_fx.phase1a_outcome_cell_summaries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION systematic_fx.require_phase1a_p4_cell_pair_release();

CREATE FUNCTION systematic_fx.require_phase1a_p4_artifact_pair_release()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    released boolean;
BEGIN
    IF NEW.artifact_type <> 'PHASE1A_OUTCOME_REPLAY_RESULT'
       OR NEW.metadata #>> '{query_id}' NOT IN
          ('p4_01_opposite_depth_depletion_continuation',
           'p4_02_depth_resistance_reversal') THEN
        RETURN NULL;
    END IF;
    SELECT EXISTS (
        SELECT 1
        FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
        JOIN systematic_fx.phase1a_p4_outcome_pair_releases AS release
          ON release.p4_01_outcome_replay_manifest_id =
                manifest.outcome_replay_manifest_id
             OR release.p4_02_outcome_replay_manifest_id =
                manifest.outcome_replay_manifest_id
        WHERE manifest.result_artifact_id = NEW.artifact_id
          AND manifest.status = 'SUCCEEDED'
    ) INTO released;
    IF NOT released THEN
        RAISE EXCEPTION 'P4 result artifacts cannot commit outside atomic pair release';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER phase1a_p4_outcome_artifacts_require_pair_release
AFTER INSERT OR UPDATE
ON systematic_fx.artifacts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION systematic_fx.require_phase1a_p4_artifact_pair_release();

COMMENT ON TABLE systematic_fx.phase1a_p4_outcome_pair_batches IS
    'Retryable PREPARED bindings that freeze both P4 attempts, configs, query definitions, input manifests, and all previously observed outcome lineage before either replay starts.';
COMMENT ON TABLE systematic_fx.phase1a_p4_outcome_pair_releases IS
    'The single append-only simultaneous publication of both P4 economic surfaces, four decisions, and their canonical release SHA-256.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (28, 'phase1a_p4_paired_outcomes', :'migration_checksum');

COMMIT;
