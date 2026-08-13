BEGIN;

CREATE TABLE systematic_fx.m0b_epochs (
    m0b_epoch_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    epoch_key text NOT NULL UNIQUE,
    epoch_sha256 text NOT NULL UNIQUE,
    canonical_epoch jsonb NOT NULL,
    campaign_id bigint NOT NULL UNIQUE,
    manifest_artifact_id bigint NOT NULL,
    manifest_artifact_sha256 text NOT NULL,
    manifest_artifact_byte_size bigint NOT NULL,
    dataset_version text NOT NULL,
    dataset_sha256 text NOT NULL,
    calendar_version text NOT NULL,
    calendar_sha256 text NOT NULL,
    contract_reference_version text NOT NULL,
    contract_reference_sha256 text NOT NULL,
    split_version text NOT NULL,
    split_sha256 text NOT NULL,
    feature_version text NOT NULL,
    feature_sha256 text NOT NULL,
    label_version text NOT NULL,
    label_sha256 text NOT NULL,
    cost_version text NOT NULL,
    cost_sha256 text NOT NULL,
    execution_version text NOT NULL,
    execution_sha256 text NOT NULL,
    engine_version text NOT NULL,
    code_commit text NOT NULL,
    code_snapshot_sha256 text NOT NULL,
    dependency_lock_sha256 text NOT NULL,
    real_candidate_budget integer NOT NULL,
    null_candidate_budget integer NOT NULL,
    max_attempts_per_candidate integer NOT NULL,
    status text NOT NULL DEFAULT 'PREPARED',
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    error_message text,
    CONSTRAINT m0b_epochs_campaign_fk FOREIGN KEY (campaign_id)
        REFERENCES systematic_fx.campaigns(campaign_id),
    CONSTRAINT m0b_epochs_manifest_fk FOREIGN KEY (manifest_artifact_id)
        REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT m0b_epochs_key_nonempty CHECK (btrim(epoch_key) <> ''),
    CONSTRAINT m0b_epochs_canonical_object CHECK (jsonb_typeof(canonical_epoch) = 'object'),
    CONSTRAINT m0b_epochs_hashes_valid CHECK (
        epoch_sha256 ~ '^[0-9a-f]{64}$'
        AND manifest_artifact_sha256 ~ '^[0-9a-f]{64}$'
        AND dataset_sha256 ~ '^[0-9a-f]{64}$'
        AND calendar_sha256 ~ '^[0-9a-f]{64}$'
        AND contract_reference_sha256 ~ '^[0-9a-f]{64}$'
        AND split_sha256 ~ '^[0-9a-f]{64}$'
        AND feature_sha256 ~ '^[0-9a-f]{64}$'
        AND label_sha256 ~ '^[0-9a-f]{64}$'
        AND cost_sha256 ~ '^[0-9a-f]{64}$'
        AND execution_sha256 ~ '^[0-9a-f]{64}$'
        AND code_snapshot_sha256 ~ '^[0-9a-f]{64}$'
        AND dependency_lock_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT m0b_epochs_versions_nonempty CHECK (
        btrim(dataset_version) <> '' AND btrim(calendar_version) <> ''
        AND btrim(contract_reference_version) <> '' AND btrim(split_version) <> ''
        AND btrim(feature_version) <> '' AND btrim(label_version) <> ''
        AND btrim(cost_version) <> '' AND btrim(execution_version) <> ''
        AND btrim(engine_version) <> ''),
    CONSTRAINT m0b_epochs_manifest_size_nonnegative CHECK (manifest_artifact_byte_size >= 0),
    CONSTRAINT m0b_epochs_code_commit_valid CHECK (
        code_commit ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'),
    CONSTRAINT m0b_epochs_budgets_positive CHECK (
        real_candidate_budget > 0 AND null_candidate_budget >= 2
        AND max_attempts_per_candidate BETWEEN 1 AND 10),
    CONSTRAINT m0b_epochs_status_valid CHECK (
        status IN ('PREPARED', 'RUNNING', 'COMPLETED', 'FAILED')),
    CONSTRAINT m0b_epochs_lifecycle_shape CHECK (
        (status = 'PREPARED' AND started_at IS NULL AND finished_at IS NULL
         AND error_message IS NULL)
        OR (status = 'RUNNING' AND started_at IS NOT NULL AND finished_at IS NULL
            AND error_message IS NULL)
        OR (status = 'COMPLETED' AND started_at IS NOT NULL AND finished_at IS NOT NULL
            AND error_message IS NULL)
        OR (status = 'FAILED' AND finished_at IS NOT NULL
            AND btrim(COALESCE(error_message, '')) <> '')),
    CONSTRAINT m0b_epochs_time_order CHECK (
        (started_at IS NULL OR started_at >= created_at)
        AND (finished_at IS NULL OR finished_at >= created_at)
        AND (started_at IS NULL OR finished_at IS NULL OR started_at <= finished_at))
);

CREATE TABLE systematic_fx.m0b_candidates (
    m0b_candidate_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    m0b_epoch_id bigint NOT NULL,
    parent_candidate_id bigint,
    research_run_spec_id bigint NOT NULL UNIQUE,
    candidate_kind text NOT NULL,
    ordinal integer NOT NULL,
    candidate_sha256 text NOT NULL,
    canonical_candidate jsonb NOT NULL,
    status text NOT NULL DEFAULT 'QUEUED',
    started_at timestamptz,
    finished_at timestamptz,
    error_message text,
    registered_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT m0b_candidates_epoch_fk FOREIGN KEY (m0b_epoch_id)
        REFERENCES systematic_fx.m0b_epochs(m0b_epoch_id),
    CONSTRAINT m0b_candidates_epoch_identity UNIQUE (m0b_epoch_id, m0b_candidate_id),
    CONSTRAINT m0b_candidates_parent_fk FOREIGN KEY (m0b_epoch_id, parent_candidate_id)
        REFERENCES systematic_fx.m0b_candidates(m0b_epoch_id, m0b_candidate_id),
    CONSTRAINT m0b_candidates_run_spec_fk FOREIGN KEY (research_run_spec_id)
        REFERENCES systematic_fx.research_run_specs(research_run_spec_id),
    CONSTRAINT m0b_candidates_identity UNIQUE (m0b_epoch_id, candidate_kind, ordinal),
    CONSTRAINT m0b_candidates_hash_unique UNIQUE (m0b_epoch_id, candidate_sha256),
    CONSTRAINT m0b_candidates_kind_valid CHECK (candidate_kind IN ('REAL', 'NULL')),
    CONSTRAINT m0b_candidates_ordinal_positive CHECK (ordinal > 0),
    CONSTRAINT m0b_candidates_sha_valid CHECK (candidate_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT m0b_candidates_canonical_object CHECK (jsonb_typeof(canonical_candidate) = 'object'),
    CONSTRAINT m0b_candidates_parent_shape CHECK (
        (candidate_kind = 'REAL' AND parent_candidate_id IS NULL)
        OR (candidate_kind = 'NULL' AND parent_candidate_id IS NOT NULL)),
    CONSTRAINT m0b_candidates_status_valid CHECK (
        status IN ('QUEUED', 'RUNNING', 'SCREENED_OUT', 'REGISTERED', 'FAILED', 'CRASHED')),
    CONSTRAINT m0b_candidates_registration_shape CHECK (
        (status = 'REGISTERED' AND registered_at IS NOT NULL)
        OR (status <> 'REGISTERED' AND registered_at IS NULL)),
    CONSTRAINT m0b_candidates_lifecycle_shape CHECK (
        (status = 'QUEUED' AND started_at IS NULL AND finished_at IS NULL
         AND error_message IS NULL)
        OR (status = 'RUNNING' AND started_at IS NOT NULL AND finished_at IS NULL
            AND error_message IS NULL)
        OR (status IN ('SCREENED_OUT', 'REGISTERED') AND started_at IS NOT NULL
            AND finished_at IS NOT NULL AND error_message IS NULL)
        OR (status IN ('FAILED', 'CRASHED') AND finished_at IS NOT NULL
            AND btrim(COALESCE(error_message, '')) <> '')),
    CONSTRAINT m0b_candidates_time_order CHECK (
        (started_at IS NULL OR started_at >= created_at)
        AND (finished_at IS NULL OR finished_at >= created_at)
        AND (started_at IS NULL OR finished_at IS NULL OR started_at <= finished_at)
        AND (registered_at IS NULL OR
             (started_at IS NOT NULL AND finished_at IS NOT NULL
              AND registered_at BETWEEN started_at AND finished_at)))
);

CREATE TABLE systematic_fx.m0b_checkpoints (
    m0b_checkpoint_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    m0b_candidate_id bigint NOT NULL,
    research_run_attempt_id bigint NOT NULL,
    checkpoint_sequence integer NOT NULL,
    checkpoint_sha256 text NOT NULL,
    predecessor_sha256 text,
    cursor jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT m0b_checkpoints_candidate_fk FOREIGN KEY (m0b_candidate_id)
        REFERENCES systematic_fx.m0b_candidates(m0b_candidate_id),
    CONSTRAINT m0b_checkpoints_attempt_fk FOREIGN KEY (research_run_attempt_id)
        REFERENCES systematic_fx.research_run_attempts(research_run_attempt_id),
    CONSTRAINT m0b_checkpoints_identity UNIQUE (research_run_attempt_id, checkpoint_sequence),
    CONSTRAINT m0b_checkpoints_sha_unique UNIQUE (checkpoint_sha256),
    CONSTRAINT m0b_checkpoints_sequence_positive CHECK (checkpoint_sequence > 0),
    CONSTRAINT m0b_checkpoints_hashes_valid CHECK (
        checkpoint_sha256 ~ '^[0-9a-f]{64}$'
        AND (predecessor_sha256 IS NULL OR predecessor_sha256 ~ '^[0-9a-f]{64}$')),
    CONSTRAINT m0b_checkpoints_predecessor_shape CHECK (
        (checkpoint_sequence = 1 AND predecessor_sha256 IS NULL)
        OR (checkpoint_sequence > 1 AND predecessor_sha256 IS NOT NULL)),
    CONSTRAINT m0b_checkpoints_cursor_object CHECK (jsonb_typeof(cursor) = 'object')
);

CREATE TABLE systematic_fx.m0b_artifact_links (
    m0b_artifact_link_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    m0b_candidate_id bigint NOT NULL,
    research_run_attempt_id bigint NOT NULL,
    artifact_id bigint NOT NULL,
    artifact_role text NOT NULL,
    artifact_sha256 text NOT NULL,
    artifact_byte_size bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT m0b_artifact_links_candidate_fk FOREIGN KEY (m0b_candidate_id)
        REFERENCES systematic_fx.m0b_candidates(m0b_candidate_id),
    CONSTRAINT m0b_artifact_links_attempt_fk FOREIGN KEY (research_run_attempt_id)
        REFERENCES systematic_fx.research_run_attempts(research_run_attempt_id),
    CONSTRAINT m0b_artifact_links_artifact_fk FOREIGN KEY (artifact_id)
        REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT m0b_artifact_links_identity UNIQUE (m0b_candidate_id, artifact_role),
    CONSTRAINT m0b_artifact_links_artifact_unique UNIQUE (artifact_id),
    CONSTRAINT m0b_artifact_links_role_valid CHECK (
        artifact_role IN ('RESULT', 'DETAIL', 'FAILURE')),
    CONSTRAINT m0b_artifact_links_sha_valid CHECK (artifact_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT m0b_artifact_links_size_nonnegative CHECK (artifact_byte_size >= 0)
);

CREATE FUNCTION systematic_fx.reject_m0b_identity_mutation()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M0b governed rows are append-preserved';
    END IF;
    RAISE EXCEPTION 'M0b governed identity is immutable';
END;
$$;

CREATE FUNCTION systematic_fx.m0b_json_has_forbidden_reference(document jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
SET search_path = pg_catalog AS $$
    WITH RECURSIVE nodes(key, value) AS (
        SELECT NULL::text, document
        UNION ALL
        SELECT child.key, child.value
          FROM nodes AS parent
          CROSS JOIN LATERAL (
              SELECT item.key, item.value
                FROM jsonb_each(
                    CASE jsonb_typeof(parent.value)
                        WHEN 'object' THEN parent.value
                        ELSE '{}'::jsonb
                    END) AS item
              UNION ALL
              SELECT NULL::text, item.value
                FROM jsonb_array_elements(
                    CASE jsonb_typeof(parent.value)
                        WHEN 'array' THEN parent.value
                        ELSE '[]'::jsonb
                    END) AS item(value)
          ) AS child
    )
    SELECT EXISTS (
        SELECT 1
          FROM nodes
         WHERE (
             key IS NOT NULL
             AND key ~* '(holdout|sealed|forward|credential|password|secret|token|api[_-]?key|(^|[_-])(path|uri)($|[_-]))'
         )
         OR (
             jsonb_typeof(value) = 'string'
             AND value #>> '{}' <> 'SEARCH_ONLY_NOT_HOLDOUT_NOT_FORWARD'
             AND (
                 value #>> '{}' ~*
                     '(holdout|sealed|credential|password|secret|token|api[ _-]?key)'
                 OR value #>> '{}' ~*
                     '(^|[[:space:]])(/|~/|[a-z][a-z0-9+.-]*:/+)'
             )
         )
    );
$$;

CREATE TRIGGER m0b_checkpoints_immutable
BEFORE UPDATE OR DELETE ON systematic_fx.m0b_checkpoints
FOR EACH ROW EXECUTE FUNCTION systematic_fx.reject_m0b_identity_mutation();
CREATE TRIGGER m0b_artifact_links_immutable
BEFORE UPDATE OR DELETE ON systematic_fx.m0b_artifact_links
FOR EACH ROW EXECUTE FUNCTION systematic_fx.reject_m0b_identity_mutation();

CREATE FUNCTION systematic_fx.m0b_artifact_is_protected(target_artifact_id bigint)
RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT EXISTS (
        SELECT 1 FROM systematic_fx.m0b_epochs AS epoch
         WHERE epoch.manifest_artifact_id = target_artifact_id
        UNION ALL
        SELECT 1 FROM systematic_fx.m0b_artifact_links AS link
         WHERE link.artifact_id = target_artifact_id
    );
$$;

CREATE FUNCTION systematic_fx.protect_m0b_artifact_bytes()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE old_protected boolean; new_protected boolean;
BEGIN
    old_protected := systematic_fx.m0b_artifact_is_protected(OLD.artifact_id)
        OR OLD.artifact_type = 'M0B_EPOCH_MANIFEST'
        OR OLD.metadata #>> '{identity_schema}' LIKE 'systematic_fx.m0b.%';
    IF old_protected THEN
        RAISE EXCEPTION 'M0b governed artifacts are immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    new_protected := NEW.artifact_type = 'M0B_EPOCH_MANIFEST'
        OR NEW.metadata #>> '{identity_schema}' LIKE 'systematic_fx.m0b.%';
    IF new_protected THEN
        RAISE EXCEPTION 'artifacts cannot be mutated into M0b governed identity';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER artifacts_protect_m0b_lineage
BEFORE UPDATE OR DELETE ON systematic_fx.artifacts
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_m0b_artifact_bytes();

CREATE FUNCTION systematic_fx.protect_m0b_dataset_identity()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE governed boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
          FROM systematic_fx.campaigns AS campaign
          JOIN systematic_fx.m0b_epochs AS epoch USING (campaign_id)
         WHERE campaign.dataset_id = OLD.dataset_id
    ) INTO governed;
    IF NOT governed THEN
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END IF;
    IF TG_OP = 'DELETE' OR NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'M0b frozen dataset identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER datasets_protect_m0b_identity
BEFORE UPDATE OR DELETE ON systematic_fx.datasets
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_m0b_dataset_identity();

CREATE FUNCTION systematic_fx.protect_m0b_campaign_identity()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE governed boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM systematic_fx.m0b_epochs
         WHERE campaign_id = OLD.campaign_id
    ) INTO governed;
    IF NOT governed THEN RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END; END IF;
    IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'M0b campaign is append-preserved'; END IF;
    IF ROW(NEW.campaign_key, NEW.dataset_id, NEW.name, NEW.selected_start_date,
           NEW.selected_end_date, NEW.roll_cutoff_date, NEW.data_manifest_sha256,
           NEW.feature_version, NEW.outcome_version, NEW.cost_model_version,
           NEW.execution_model_version, NEW.code_commit, NEW.config_sha256,
           NEW.split_policy, NEW.trial_budget, NEW.finalist_budget, NEW.created_at,
           NEW.frozen_at, NEW.holdout_revealed_at, NEW.closed_at)
       IS DISTINCT FROM
       ROW(OLD.campaign_key, OLD.dataset_id, OLD.name, OLD.selected_start_date,
           OLD.selected_end_date, OLD.roll_cutoff_date, OLD.data_manifest_sha256,
           OLD.feature_version, OLD.outcome_version, OLD.cost_model_version,
           OLD.execution_model_version, OLD.code_commit, OLD.config_sha256,
           OLD.split_policy, OLD.trial_budget, OLD.finalist_budget, OLD.created_at,
           OLD.frozen_at, OLD.holdout_revealed_at, OLD.closed_at) THEN
        RAISE EXCEPTION 'M0b frozen campaign identity is immutable';
    END IF;
    IF OLD.status = 'FROZEN' AND NEW.status NOT IN ('FROZEN', 'RUNNING') THEN
        RAISE EXCEPTION 'invalid M0b campaign transition';
    END IF;
    IF OLD.status = 'RUNNING' AND NEW.status <> 'RUNNING' THEN
        RAISE EXCEPTION 'M0b campaign close/promotion requires a later authorized migration';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER campaigns_protect_m0b_identity
BEFORE UPDATE OR DELETE ON systematic_fx.campaigns
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_m0b_campaign_identity();

CREATE FUNCTION systematic_fx.protect_m0b_experiment_identity()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE governed boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
          FROM systematic_fx.research_run_specs AS run_spec
          JOIN systematic_fx.m0b_candidates AS candidate
            USING (research_run_spec_id)
         WHERE run_spec.experiment_id = OLD.experiment_id
    ) INTO governed;
    IF NOT governed THEN RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END; END IF;
    IF TG_OP = 'DELETE' OR NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'M0b frozen experiment identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER experiments_protect_m0b_identity
BEFORE UPDATE OR DELETE ON systematic_fx.experiments
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_m0b_experiment_identity();

CREATE FUNCTION systematic_fx.protect_m0b_epoch()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M0b epochs are append-preserved';
    END IF;
    IF ROW(NEW.epoch_key, NEW.epoch_sha256, NEW.canonical_epoch,
           NEW.campaign_id, NEW.manifest_artifact_id,
           NEW.manifest_artifact_sha256, NEW.manifest_artifact_byte_size,
           NEW.dataset_version, NEW.dataset_sha256,
           NEW.calendar_version, NEW.calendar_sha256,
           NEW.contract_reference_version, NEW.contract_reference_sha256,
           NEW.split_version, NEW.split_sha256,
           NEW.feature_version, NEW.feature_sha256,
           NEW.label_version, NEW.label_sha256,
           NEW.cost_version, NEW.cost_sha256,
           NEW.execution_version, NEW.execution_sha256, NEW.engine_version,
           NEW.code_commit, NEW.code_snapshot_sha256, NEW.dependency_lock_sha256,
           NEW.real_candidate_budget, NEW.null_candidate_budget,
           NEW.max_attempts_per_candidate, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.epoch_key, OLD.epoch_sha256, OLD.canonical_epoch,
           OLD.campaign_id, OLD.manifest_artifact_id,
           OLD.manifest_artifact_sha256, OLD.manifest_artifact_byte_size,
           OLD.dataset_version, OLD.dataset_sha256,
           OLD.calendar_version, OLD.calendar_sha256,
           OLD.contract_reference_version, OLD.contract_reference_sha256,
           OLD.split_version, OLD.split_sha256,
           OLD.feature_version, OLD.feature_sha256,
           OLD.label_version, OLD.label_sha256,
           OLD.cost_version, OLD.cost_sha256,
           OLD.execution_version, OLD.execution_sha256, OLD.engine_version,
           OLD.code_commit, OLD.code_snapshot_sha256, OLD.dependency_lock_sha256,
           OLD.real_candidate_budget, OLD.null_candidate_budget,
           OLD.max_attempts_per_candidate, OLD.created_at) THEN
        RAISE EXCEPTION 'M0b epoch identity is immutable';
    END IF;
    IF OLD.status IN ('COMPLETED', 'FAILED') THEN
        RAISE EXCEPTION 'terminal M0b epochs are immutable';
    END IF;
    IF (OLD.status, NEW.status) NOT IN (('PREPARED', 'RUNNING'), ('PREPARED', 'FAILED'),
                                        ('RUNNING', 'COMPLETED'), ('RUNNING', 'FAILED')) THEN
        RAISE EXCEPTION 'invalid M0b epoch transition';
    END IF;
    IF OLD.status = 'RUNNING' AND NEW.started_at IS DISTINCT FROM OLD.started_at THEN
        RAISE EXCEPTION 'M0b epoch start time is immutable once running';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER m0b_epochs_preserve
BEFORE UPDATE OR DELETE ON systematic_fx.m0b_epochs
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_m0b_epoch();

CREATE FUNCTION systematic_fx.validate_m0b_epoch_insert()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    campaign_record record;
    artifact_record record;
BEGIN
    IF NEW.status <> 'PREPARED' OR NEW.started_at IS NOT NULL OR NEW.finished_at IS NOT NULL
       OR NEW.error_message IS NOT NULL THEN
        RAISE EXCEPTION 'M0b epoch must be inserted PREPARED';
    END IF;
    IF systematic_fx.canonical_jsonb_sha256(NEW.canonical_epoch)
           IS DISTINCT FROM NEW.epoch_sha256
       OR systematic_fx.m0b_json_has_forbidden_reference(NEW.canonical_epoch)
       OR (SELECT array_agg(key ORDER BY key)
             FROM jsonb_object_keys(NEW.canonical_epoch) AS key)
            IS DISTINCT FROM ARRAY[
                'admission_rules', 'artifact_schema', 'authority', 'budgets',
                'calendar', 'code_commit', 'code_snapshot_sha256',
                'contract_reference', 'cost', 'dataset',
                'dependency_lock_sha256', 'engine_version', 'epoch_key',
                'execution', 'execution_assumptions', 'feature', 'label',
                'null_controls', 'parent_epoch', 'random_seeds', 'retry',
                'roll_policy', 'search_space', 'session_policy', 'split',
                'strategy_families']::text[]
       OR NEW.canonical_epoch #>> '{artifact_schema}'
           IS DISTINCT FROM 'systematic_fx.m0b_epoch.v1'
       OR NEW.canonical_epoch #>> '{epoch_key}' IS DISTINCT FROM NEW.epoch_key
       OR NEW.canonical_epoch -> 'dataset' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.dataset_version, 'sha256', NEW.dataset_sha256)
       OR NEW.canonical_epoch -> 'calendar' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.calendar_version, 'sha256', NEW.calendar_sha256)
       OR NEW.canonical_epoch -> 'contract_reference' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.contract_reference_version,
            'sha256', NEW.contract_reference_sha256)
       OR NEW.canonical_epoch -> 'split' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.split_version, 'sha256', NEW.split_sha256)
       OR NEW.canonical_epoch -> 'feature' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.feature_version, 'sha256', NEW.feature_sha256)
       OR NEW.canonical_epoch -> 'label' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.label_version, 'sha256', NEW.label_sha256)
       OR NEW.canonical_epoch -> 'cost' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.cost_version, 'sha256', NEW.cost_sha256)
       OR NEW.canonical_epoch -> 'execution' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.execution_version, 'sha256', NEW.execution_sha256)
       OR NEW.canonical_epoch #>> '{engine_version}' IS DISTINCT FROM NEW.engine_version
       OR NEW.canonical_epoch #>> '{code_commit}' IS DISTINCT FROM NEW.code_commit
       OR NEW.canonical_epoch #>> '{code_snapshot_sha256}'
            IS DISTINCT FROM NEW.code_snapshot_sha256
       OR NEW.canonical_epoch #>> '{dependency_lock_sha256}'
            IS DISTINCT FROM NEW.dependency_lock_sha256
       OR NEW.canonical_epoch #>> '{authority}'
            IS DISTINCT FROM 'SEARCH_ONLY_NOT_HOLDOUT_NOT_FORWARD'
       OR NEW.canonical_epoch -> 'parent_epoch' IS DISTINCT FROM 'null'::jsonb
       OR jsonb_typeof(NEW.canonical_epoch -> 'strategy_families')
            IS DISTINCT FROM 'array'
       OR jsonb_array_length(NEW.canonical_epoch -> 'strategy_families') = 0
       OR NEW.canonical_epoch -> 'admission_rules'
            IS DISTINCT FROM '{"maximum_authority":"REGISTER"}'::jsonb
       OR jsonb_typeof(NEW.canonical_epoch -> 'search_space')
            IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key)
             FROM jsonb_object_keys(NEW.canonical_epoch -> 'search_space') AS key)
            IS DISTINCT FROM ARRAY['barrier_grid', 'parameter_ranges']::text[]
       OR jsonb_typeof(NEW.canonical_epoch #> '{search_space,parameter_ranges}')
            IS DISTINCT FROM 'object'
       OR NEW.canonical_epoch #> '{search_space,parameter_ranges}' = '{}'::jsonb
       OR EXISTS (
            SELECT 1
              FROM jsonb_each(NEW.canonical_epoch #> '{search_space,parameter_ranges}') item
             WHERE jsonb_typeof(item.value) IS DISTINCT FROM 'array'
                OR jsonb_array_length(item.value) = 0)
       OR jsonb_typeof(NEW.canonical_epoch #> '{search_space,barrier_grid}')
            IS DISTINCT FROM 'object'
       OR NEW.canonical_epoch #> '{search_space,barrier_grid}' = '{}'::jsonb
       OR EXISTS (
            SELECT 1
              FROM jsonb_each(NEW.canonical_epoch #> '{search_space,barrier_grid}') item
             WHERE jsonb_typeof(item.value) IS DISTINCT FROM 'array'
                OR jsonb_array_length(item.value) = 0)
       OR jsonb_typeof(NEW.canonical_epoch -> 'random_seeds')
            IS DISTINCT FROM 'array'
       OR jsonb_array_length(NEW.canonical_epoch -> 'random_seeds') = 0
       OR NEW.canonical_epoch -> 'null_controls' IS DISTINCT FROM
            '["CIRCULAR_TIME_SHIFT","MATCHED_RANDOM_ENTRY"]'::jsonb
       OR NEW.canonical_epoch -> 'execution_assumptions' IS DISTINCT FROM
            '{"entry_latency":"NEXT_ELIGIBLE_QUOTE_PLUS_ONE_ADVERSE_TICK","passive_tp_fill":"TRADE_THROUGH_ONLY","stop_execution":"MARKETABLE_CONSERVATIVE"}'::jsonb
       OR NEW.canonical_epoch #>> '{session_policy}'
            IS DISTINCT FROM 'NO_CROSS_CLOSED_MARKET'
       OR NEW.canonical_epoch -> 'roll_policy' IS DISTINCT FROM
            '{"selection":"PREVIOUS_DAY_VOLUME","hold_same_instrument_until_exit":true,"no_entry_inside_roll_guard":true}'::jsonb
       OR NEW.canonical_epoch -> 'budgets' IS DISTINCT FROM jsonb_build_object(
            'real', NEW.real_candidate_budget, 'null', NEW.null_candidate_budget)
       OR NEW.canonical_epoch -> 'retry' IS DISTINCT FROM jsonb_build_object(
            'max_attempts_per_candidate', NEW.max_attempts_per_candidate) THEN
        RAISE EXCEPTION 'M0b canonical epoch identity mismatch';
    END IF;
    SELECT campaign.status, campaign.frozen_at, campaign.holdout_revealed_at,
           campaign.closed_at, campaign.trial_budget,
           data_manifest_sha256, feature_version, outcome_version,
           cost_model_version, execution_model_version, code_commit,
           dataset.status AS dataset_status,
           dataset.manifest_sha256 AS dataset_manifest_sha256,
           dataset.metadata AS dataset_metadata
      INTO STRICT campaign_record
      FROM systematic_fx.campaigns AS campaign
      JOIN systematic_fx.datasets AS dataset USING (dataset_id)
     WHERE campaign.campaign_id = NEW.campaign_id
     FOR UPDATE OF campaign FOR SHARE OF dataset;
    IF campaign_record.status NOT IN ('FROZEN', 'RUNNING')
       OR campaign_record.frozen_at IS NULL
       OR campaign_record.holdout_revealed_at IS NOT NULL
       OR campaign_record.closed_at IS NOT NULL THEN
        RAISE EXCEPTION 'M0b epoch requires a frozen open unrevealed campaign';
    END IF;
    IF NEW.real_candidate_budget + NEW.null_candidate_budget > campaign_record.trial_budget THEN
        RAISE EXCEPTION 'M0b epoch budgets exceed the frozen campaign budget';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM systematic_fx.research_run_specs
         WHERE campaign_id = NEW.campaign_id
    ) THEN
        RAISE EXCEPTION 'M0b epoch requires a pristine campaign with no pre-existing RunSpecs';
    END IF;
    IF campaign_record.dataset_status IS DISTINCT FROM 'READY'
       OR campaign_record.dataset_manifest_sha256 IS DISTINCT FROM NEW.dataset_sha256
       OR campaign_record.dataset_metadata ->> 'dataset_version'
            IS DISTINCT FROM NEW.dataset_version
       OR campaign_record.dataset_metadata ->> 'data_role' IS DISTINCT FROM 'SEARCH'
       OR campaign_record.data_manifest_sha256 IS DISTINCT FROM NEW.dataset_sha256
       OR campaign_record.feature_version IS DISTINCT FROM NEW.feature_version
       OR campaign_record.outcome_version IS DISTINCT FROM NEW.label_version
       OR campaign_record.cost_model_version IS DISTINCT FROM NEW.cost_version
       OR campaign_record.execution_model_version IS DISTINCT FROM NEW.execution_version
       OR campaign_record.code_commit IS DISTINCT FROM NEW.code_commit THEN
        RAISE EXCEPTION 'M0b epoch identity differs from its frozen campaign';
    END IF;
    SELECT artifact_type, sha256, byte_size, metadata INTO STRICT artifact_record
      FROM systematic_fx.artifacts WHERE artifact_id = NEW.manifest_artifact_id FOR SHARE;
    IF artifact_record.artifact_type <> 'M0B_EPOCH_MANIFEST'
       OR artifact_record.sha256 IS DISTINCT FROM NEW.manifest_artifact_sha256
       OR artifact_record.byte_size IS DISTINCT FROM NEW.manifest_artifact_byte_size
       OR artifact_record.metadata ->> 'epoch_sha256' IS DISTINCT FROM NEW.epoch_sha256
       OR artifact_record.metadata ->> 'identity_schema'
            IS DISTINCT FROM 'systematic_fx.m0b.epoch_manifest.v1' THEN
        RAISE EXCEPTION 'M0b epoch manifest artifact identity mismatch';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER m0b_epochs_validate_insert
BEFORE INSERT ON systematic_fx.m0b_epochs
FOR EACH ROW EXECUTE FUNCTION systematic_fx.validate_m0b_epoch_insert();

CREATE FUNCTION systematic_fx.protect_m0b_candidate()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M0b candidates are append-preserved';
    END IF;
    IF ROW(NEW.m0b_epoch_id, NEW.parent_candidate_id, NEW.research_run_spec_id,
           NEW.candidate_kind, NEW.ordinal, NEW.candidate_sha256,
           NEW.canonical_candidate, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.m0b_epoch_id, OLD.parent_candidate_id, OLD.research_run_spec_id,
           OLD.candidate_kind, OLD.ordinal, OLD.candidate_sha256,
           OLD.canonical_candidate, OLD.created_at) THEN
        RAISE EXCEPTION 'M0b candidate identity is immutable';
    END IF;
    IF OLD.status IN ('SCREENED_OUT', 'REGISTERED', 'FAILED', 'CRASHED') THEN
        RAISE EXCEPTION 'terminal M0b candidates are immutable';
    END IF;
    IF (OLD.status, NEW.status) NOT IN (('QUEUED', 'RUNNING'), ('QUEUED', 'FAILED'),
         ('QUEUED', 'CRASHED'), ('RUNNING', 'SCREENED_OUT'), ('RUNNING', 'REGISTERED'),
         ('RUNNING', 'FAILED'), ('RUNNING', 'CRASHED')) THEN
        RAISE EXCEPTION 'invalid M0b candidate transition';
    END IF;
    IF OLD.status = 'RUNNING' AND NEW.started_at IS DISTINCT FROM OLD.started_at THEN
        RAISE EXCEPTION 'M0b candidate start time is immutable once running';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER m0b_candidates_preserve
BEFORE UPDATE OR DELETE ON systematic_fx.m0b_candidates
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_m0b_candidate();

CREATE FUNCTION systematic_fx.validate_m0b_candidate_update_context()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE epoch_status text; campaign_status text; holdout_revealed_at timestamptz;
        closed_at timestamptz;
BEGIN
    SELECT epoch.status, campaign.status, campaign.holdout_revealed_at,
           campaign.closed_at
      INTO STRICT epoch_status, campaign_status, holdout_revealed_at, closed_at
      FROM systematic_fx.m0b_epochs AS epoch
      JOIN systematic_fx.campaigns AS campaign USING (campaign_id)
     WHERE epoch.m0b_epoch_id = NEW.m0b_epoch_id FOR SHARE;
    IF epoch_status <> 'RUNNING' OR campaign_status <> 'RUNNING'
       OR holdout_revealed_at IS NOT NULL OR closed_at IS NOT NULL THEN
        RAISE EXCEPTION 'M0b candidate updates require active unrevealed search';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER m0b_candidates_validate_update_context
BEFORE UPDATE ON systematic_fx.m0b_candidates
FOR EACH ROW EXECUTE FUNCTION systematic_fx.validate_m0b_candidate_update_context();

CREATE FUNCTION systematic_fx.validate_m0b_candidate_insert()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    epoch_record record;
    run_record record;
    current_count integer;
    parent_record record;
    experiment_record record;
    preexisting_attempt_count integer;
    canonical_parent_sha256 text;
    search_item record;
BEGIN
    SELECT e.*, c.status AS campaign_status, c.frozen_at,
           c.holdout_revealed_at, c.closed_at
      INTO STRICT epoch_record
      FROM systematic_fx.m0b_epochs e
      JOIN systematic_fx.campaigns c ON c.campaign_id = e.campaign_id
     WHERE e.m0b_epoch_id = NEW.m0b_epoch_id
     FOR UPDATE OF e;
    IF epoch_record.status NOT IN ('PREPARED', 'RUNNING')
       OR epoch_record.campaign_status NOT IN ('FROZEN', 'RUNNING')
       OR epoch_record.frozen_at IS NULL
       OR epoch_record.holdout_revealed_at IS NOT NULL OR epoch_record.closed_at IS NOT NULL THEN
        RAISE EXCEPTION 'M0b candidates require an open unrevealed search epoch';
    END IF;
    IF systematic_fx.m0b_json_has_forbidden_reference(NEW.canonical_candidate) THEN
        RAISE EXCEPTION 'M0b candidate contains a forbidden external reference';
    END IF;
    SELECT campaign_id, run_kind, engine_version, canonical_spec,
           source_manifest_hashes,
           experiment_id, parent_run_spec_id, deterministic_seed, direction,
           cost_version, cost_sha256,
           eligible_calendar_version, eligible_calendar_sha256,
           split_version, split_sha256, feature_version, feature_sha256,
           outcome_version, outcome_sha256, execution_version, execution_sha256,
           code_commit, code_snapshot_sha256, dependency_lock_sha256
      INTO STRICT run_record
      FROM systematic_fx.research_run_specs
     WHERE research_run_spec_id = NEW.research_run_spec_id FOR SHARE;
    IF run_record.campaign_id <> epoch_record.campaign_id
       OR run_record.experiment_id IS NULL
       OR run_record.run_kind <> 'SCREEN'
       OR run_record.engine_version IS DISTINCT FROM epoch_record.engine_version
       OR run_record.canonical_spec #>> '{parameters,data_role}' IS DISTINCT FROM 'SEARCH'
       OR run_record.canonical_spec #>> '{parameters,split_role}' IS DISTINCT FROM 'DISCOVERY'
       OR run_record.canonical_spec #>> '{parameters,m0b_epoch_sha256}'
            IS DISTINCT FROM epoch_record.epoch_sha256
       OR run_record.canonical_spec #>> '{parameters,m0b_dataset_sha256}'
            IS DISTINCT FROM epoch_record.dataset_sha256
       OR run_record.canonical_spec #>> '{parameters,m0b_contract_reference_sha256}'
            IS DISTINCT FROM epoch_record.contract_reference_sha256
       OR run_record.canonical_spec #>> '{parameters,m0b_candidate_sha256}'
            IS DISTINCT FROM NEW.candidate_sha256
       OR run_record.source_manifest_hashes ->> 'dataset'
            IS DISTINCT FROM epoch_record.dataset_sha256
       OR run_record.source_manifest_hashes
            IS DISTINCT FROM jsonb_build_object('dataset', epoch_record.dataset_sha256)
       OR run_record.eligible_calendar_version IS DISTINCT FROM epoch_record.calendar_version
       OR run_record.eligible_calendar_sha256 IS DISTINCT FROM epoch_record.calendar_sha256
       OR run_record.split_version IS DISTINCT FROM epoch_record.split_version
       OR run_record.split_sha256 IS DISTINCT FROM epoch_record.split_sha256
       OR run_record.feature_version IS DISTINCT FROM epoch_record.feature_version
       OR run_record.feature_sha256 IS DISTINCT FROM epoch_record.feature_sha256
       OR run_record.outcome_version IS DISTINCT FROM epoch_record.label_version
       OR run_record.outcome_sha256 IS DISTINCT FROM epoch_record.label_sha256
       OR run_record.execution_version IS DISTINCT FROM epoch_record.execution_version
       OR run_record.execution_sha256 IS DISTINCT FROM epoch_record.execution_sha256
       OR run_record.code_commit IS DISTINCT FROM epoch_record.code_commit
       OR run_record.code_snapshot_sha256 IS DISTINCT FROM epoch_record.code_snapshot_sha256
       OR run_record.deterministic_seed::text
            IS DISTINCT FROM NEW.canonical_candidate #>> '{random_seed}'
       OR run_record.direction
            IS DISTINCT FROM NEW.canonical_candidate #>> '{direction}'
       OR run_record.cost_version
            IS DISTINCT FROM NEW.canonical_candidate #>> '{cost,version}'
       OR run_record.cost_sha256
            IS DISTINCT FROM NEW.canonical_candidate #>> '{cost,sha256}'
       OR run_record.cost_version IS DISTINCT FROM epoch_record.cost_version
       OR run_record.cost_sha256 IS DISTINCT FROM epoch_record.cost_sha256
       OR run_record.dependency_lock_sha256
            IS DISTINCT FROM epoch_record.dependency_lock_sha256
       OR run_record.canonical_spec #>> '{signal_policy,family}'
            IS DISTINCT FROM NEW.canonical_candidate #>> '{family_id}'
       OR NEW.canonical_candidate #>> '{artifact_schema}'
            IS DISTINCT FROM 'systematic_fx.m0b_candidate.v1'
       OR NEW.canonical_candidate #>> '{candidate_kind}'
            IS DISTINCT FROM NEW.candidate_kind
       OR (NEW.canonical_candidate #>> '{ordinal}')::integer
            IS DISTINCT FROM NEW.ordinal
       OR systematic_fx.canonical_jsonb_sha256(NEW.canonical_candidate)
            IS DISTINCT FROM NEW.candidate_sha256 THEN
        RAISE EXCEPTION 'M0b RunSpec/candidate provenance differs from its frozen epoch';
    END IF;
    IF NOT (epoch_record.canonical_epoch -> 'random_seeds'
            @> jsonb_build_array(NEW.canonical_candidate -> 'random_seed')) THEN
        RAISE EXCEPTION 'M0b candidate seed is outside its frozen search space';
    END IF;
    IF (SELECT array_agg(key ORDER BY key)
          FROM jsonb_object_keys(NEW.canonical_candidate) key)
       IS DISTINCT FROM
       (CASE NEW.candidate_kind
           WHEN 'REAL' THEN ARRAY[
               'artifact_schema', 'barrier', 'candidate_kind', 'cost', 'direction',
               'family_id', 'ordinal', 'parameters', 'random_seed']::text[]
           ELSE ARRAY[
               'artifact_schema', 'barrier', 'candidate_kind', 'control', 'cost',
               'direction', 'family_id', 'null_control', 'ordinal',
               'parameters', 'parent_candidate_sha256', 'random_seed']::text[]
       END)
       OR NEW.canonical_candidate -> 'cost' IS DISTINCT FROM jsonb_build_object(
            'version', epoch_record.cost_version,
            'sha256', epoch_record.cost_sha256) THEN
        RAISE EXCEPTION 'M0b candidate canonical keys differ from the frozen contract';
    END IF;
    IF (SELECT array_agg(key ORDER BY key)
          FROM jsonb_object_keys(NEW.canonical_candidate -> 'parameters') key)
       IS DISTINCT FROM
       (SELECT array_agg(key ORDER BY key)
          FROM jsonb_object_keys(
               epoch_record.canonical_epoch #> '{search_space,parameter_ranges}') key)
       OR (SELECT array_agg(key ORDER BY key)
             FROM jsonb_object_keys(NEW.canonical_candidate -> 'barrier') key)
          IS DISTINCT FROM
          (SELECT array_agg(key ORDER BY key)
             FROM jsonb_object_keys(
                  epoch_record.canonical_epoch #> '{search_space,barrier_grid}') key) THEN
        RAISE EXCEPTION 'M0b candidate search keys differ from its frozen search space';
    END IF;
    FOR search_item IN
        SELECT key, value
          FROM jsonb_each(epoch_record.canonical_epoch #> '{search_space,parameter_ranges}')
    LOOP
        IF NOT (search_item.value
                @> jsonb_build_array(NEW.canonical_candidate #> ARRAY['parameters', search_item.key]))
           OR NEW.canonical_candidate #> ARRAY['parameters', search_item.key] IS NULL THEN
            RAISE EXCEPTION 'M0b candidate parameter is outside its frozen search space';
        END IF;
    END LOOP;
    FOR search_item IN
        SELECT key, value
          FROM jsonb_each(epoch_record.canonical_epoch #> '{search_space,barrier_grid}')
    LOOP
        IF NOT (search_item.value
                @> jsonb_build_array(NEW.canonical_candidate #> ARRAY['barrier', search_item.key]))
           OR NEW.canonical_candidate #> ARRAY['barrier', search_item.key] IS NULL THEN
            RAISE EXCEPTION 'M0b candidate barrier is outside its frozen search space';
        END IF;
    END LOOP;
    IF EXISTS (
        SELECT 1
          FROM systematic_fx.m0b_candidates AS existing
         WHERE existing.m0b_epoch_id = NEW.m0b_epoch_id
           AND existing.candidate_sha256 = NEW.candidate_sha256
           AND existing.parent_candidate_id IS NOT DISTINCT FROM NEW.parent_candidate_id
           AND existing.research_run_spec_id = NEW.research_run_spec_id
           AND existing.candidate_kind = NEW.candidate_kind
           AND existing.ordinal = NEW.ordinal
           AND existing.canonical_candidate = NEW.canonical_candidate
    ) THEN
        RETURN NEW;
    END IF;
    SELECT primary_family, status, direction, code_commit, trial_budget
      INTO STRICT experiment_record
      FROM systematic_fx.experiments
     WHERE experiment_id = run_record.experiment_id
       AND campaign_id = epoch_record.campaign_id FOR SHARE;
    IF experiment_record.status <> 'FROZEN'
       OR experiment_record.primary_family
            IS DISTINCT FROM NEW.canonical_candidate #>> '{family_id}'
       OR NOT EXISTS (
            SELECT 1
              FROM jsonb_array_elements_text(
                   epoch_record.canonical_epoch -> 'strategy_families') AS family(value)
             WHERE family.value = NEW.canonical_candidate #>> '{family_id}'
       )
       OR experiment_record.direction NOT IN ('BOTH', run_record.direction)
       OR experiment_record.code_commit IS DISTINCT FROM epoch_record.code_commit
       OR experiment_record.trial_budget <
            epoch_record.real_candidate_budget + epoch_record.null_candidate_budget THEN
        RAISE EXCEPTION 'M0b candidate experiment differs from its frozen epoch';
    END IF;
    SELECT count(*)::integer INTO preexisting_attempt_count
      FROM systematic_fx.research_run_attempts
     WHERE research_run_spec_id = NEW.research_run_spec_id;
    IF preexisting_attempt_count <> 0 THEN
        RAISE EXCEPTION 'M0b candidate RunSpec must have no pre-existing attempts';
    END IF;
    IF NEW.candidate_kind = 'NULL' THEN
        SELECT candidate_kind, candidate_sha256, research_run_spec_id,
               canonical_candidate
          INTO STRICT parent_record
          FROM systematic_fx.m0b_candidates
         WHERE m0b_candidate_id = NEW.parent_candidate_id FOR SHARE;
        canonical_parent_sha256 := NEW.canonical_candidate #>> '{parent_candidate_sha256}';
        IF parent_record.candidate_kind <> 'REAL'
           OR parent_record.candidate_sha256 IS DISTINCT FROM canonical_parent_sha256
           OR run_record.parent_run_spec_id IS DISTINCT FROM parent_record.research_run_spec_id
           OR NEW.canonical_candidate #>> '{family_id}'
                IS DISTINCT FROM parent_record.canonical_candidate #>> '{family_id}'
           OR NEW.canonical_candidate #>> '{direction}'
                IS DISTINCT FROM parent_record.canonical_candidate #>> '{direction}'
           OR NEW.canonical_candidate -> 'cost'
                IS DISTINCT FROM parent_record.canonical_candidate -> 'cost'
           OR NEW.canonical_candidate -> 'parameters'
                IS DISTINCT FROM parent_record.canonical_candidate -> 'parameters'
           OR NEW.canonical_candidate -> 'barrier'
                IS DISTINCT FROM parent_record.canonical_candidate -> 'barrier'
           OR NEW.canonical_candidate #>> '{control}'
                IS DISTINCT FROM NEW.canonical_candidate #>> '{null_control}'
           OR NOT (epoch_record.canonical_epoch -> 'null_controls'
                @> jsonb_build_array(NEW.canonical_candidate -> 'null_control')) THEN
            RAISE EXCEPTION 'M0b NULL parent must be REAL';
        END IF;
    ELSIF run_record.parent_run_spec_id IS NOT NULL
          OR NEW.canonical_candidate ? 'parent_candidate_sha256' THEN
        RAISE EXCEPTION 'M0b REAL candidate cannot bind parent lineage';
    END IF;
    SELECT count(*)::integer INTO current_count FROM systematic_fx.m0b_candidates
     WHERE m0b_epoch_id = NEW.m0b_epoch_id AND candidate_kind = NEW.candidate_kind;
    IF NEW.status <> 'QUEUED' OR NEW.started_at IS NOT NULL OR NEW.finished_at IS NOT NULL
       OR NEW.error_message IS NOT NULL OR NEW.registered_at IS NOT NULL THEN
        RAISE EXCEPTION 'M0b candidate must be inserted QUEUED';
    END IF;
    IF NEW.ordinal > (CASE NEW.candidate_kind
                          WHEN 'REAL' THEN epoch_record.real_candidate_budget
                          ELSE epoch_record.null_candidate_budget
                      END) THEN
        RAISE EXCEPTION 'M0b candidate ordinal exceeds its budget';
    END IF;
    IF current_count >= (CASE NEW.candidate_kind
                             WHEN 'REAL' THEN epoch_record.real_candidate_budget
                             ELSE epoch_record.null_candidate_budget
                         END) THEN
        RAISE EXCEPTION 'M0b candidate budget exhausted';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER m0b_candidates_validate_insert
BEFORE INSERT ON systematic_fx.m0b_candidates
FOR EACH ROW EXECUTE FUNCTION systematic_fx.validate_m0b_candidate_insert();

CREATE FUNCTION systematic_fx.validate_m0b_checkpoint_insert()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE expected_spec bigint; actual_spec bigint; expected_predecessor text; expected_sequence integer;
        candidate_status text; attempt_status text; epoch_status text; campaign_status text;
        holdout_revealed_at timestamptz; closed_at timestamptz;
BEGIN
    SELECT candidate.research_run_spec_id, candidate.status, epoch.status,
           campaign.status, campaign.holdout_revealed_at, campaign.closed_at
      INTO STRICT expected_spec, candidate_status, epoch_status, campaign_status,
           holdout_revealed_at, closed_at
      FROM systematic_fx.m0b_candidates AS candidate
      JOIN systematic_fx.m0b_epochs AS epoch USING (m0b_epoch_id)
      JOIN systematic_fx.campaigns AS campaign USING (campaign_id)
     WHERE candidate.m0b_candidate_id = NEW.m0b_candidate_id FOR SHARE;
    SELECT research_run_spec_id, status INTO STRICT actual_spec, attempt_status
      FROM systematic_fx.research_run_attempts
     WHERE research_run_attempt_id = NEW.research_run_attempt_id FOR SHARE;
    IF actual_spec <> expected_spec THEN RAISE EXCEPTION 'M0b checkpoint attempt/candidate mismatch'; END IF;
    IF candidate_status <> 'RUNNING' OR attempt_status <> 'RUNNING'
       OR epoch_status <> 'RUNNING'
       OR campaign_status <> 'RUNNING' OR holdout_revealed_at IS NOT NULL
       OR closed_at IS NOT NULL THEN
        RAISE EXCEPTION 'M0b checkpoints require an active unrevealed search attempt';
    END IF;
    SELECT COALESCE(max(checkpoint_sequence), 0) + 1,
           (array_agg(checkpoint_sha256 ORDER BY checkpoint_sequence DESC))[1]
      INTO expected_sequence, expected_predecessor
      FROM systematic_fx.m0b_checkpoints
     WHERE research_run_attempt_id = NEW.research_run_attempt_id;
    IF NEW.checkpoint_sequence <> expected_sequence
       OR NEW.predecessor_sha256 IS DISTINCT FROM expected_predecessor THEN
        RAISE EXCEPTION 'M0b checkpoint chain is not contiguous';
    END IF;
    IF systematic_fx.m0b_json_has_forbidden_reference(NEW.cursor) THEN
        RAISE EXCEPTION 'M0b checkpoint contains a forbidden external reference';
    END IF;
    IF (SELECT array_agg(key ORDER BY key)
          FROM jsonb_object_keys(NEW.cursor) AS key)
            IS DISTINCT FROM ARRAY[
                'artifact_schema', 'checkpoint_sequence', 'm0b_candidate_id',
                'predecessor_sha256', 'research_run_attempt_id', 'state']::text[]
       OR systematic_fx.canonical_jsonb_sha256(NEW.cursor)
           IS DISTINCT FROM NEW.checkpoint_sha256
       OR NEW.cursor #>> '{artifact_schema}'
            IS DISTINCT FROM 'systematic_fx.m0b_checkpoint.v1'
       OR (NEW.cursor #>> '{m0b_candidate_id}')::bigint
            IS DISTINCT FROM NEW.m0b_candidate_id
       OR (NEW.cursor #>> '{research_run_attempt_id}')::bigint
            IS DISTINCT FROM NEW.research_run_attempt_id
       OR (NEW.cursor #>> '{checkpoint_sequence}')::integer
            IS DISTINCT FROM NEW.checkpoint_sequence
       OR NEW.cursor -> 'predecessor_sha256'
            IS DISTINCT FROM COALESCE(to_jsonb(NEW.predecessor_sha256), 'null'::jsonb)
       OR jsonb_typeof(NEW.cursor -> 'state') IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'M0b checkpoint canonical identity mismatch';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER m0b_checkpoints_validate_insert
BEFORE INSERT ON systematic_fx.m0b_checkpoints
FOR EACH ROW EXECUTE FUNCTION systematic_fx.validate_m0b_checkpoint_insert();

CREATE FUNCTION systematic_fx.validate_m0b_artifact_link_insert()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE expected_spec bigint; actual_spec bigint; attempt_status text;
        candidate_status text; epoch_status text; campaign_status text;
        holdout_revealed_at timestamptz; closed_at timestamptz;
        artifact_record record; attempt_result_artifact_id bigint;
        m0b_epoch_id bigint;
        epoch_sha256 text; candidate_sha256 text; admission_rules_sha256 text;
BEGIN
    SELECT candidate.research_run_spec_id, candidate.status, epoch.status,
           campaign.status, campaign.holdout_revealed_at, campaign.closed_at,
           epoch.m0b_epoch_id, epoch.epoch_sha256, candidate.candidate_sha256,
           systematic_fx.canonical_jsonb_sha256(
               epoch.canonical_epoch -> 'admission_rules')
      INTO STRICT expected_spec, candidate_status, epoch_status, campaign_status,
           holdout_revealed_at, closed_at, m0b_epoch_id, epoch_sha256,
           candidate_sha256, admission_rules_sha256
      FROM systematic_fx.m0b_candidates AS candidate
      JOIN systematic_fx.m0b_epochs AS epoch USING (m0b_epoch_id)
      JOIN systematic_fx.campaigns AS campaign USING (campaign_id)
     WHERE candidate.m0b_candidate_id = NEW.m0b_candidate_id FOR SHARE;
    SELECT research_run_spec_id, status, result_artifact_id
      INTO STRICT actual_spec, attempt_status, attempt_result_artifact_id
      FROM systematic_fx.research_run_attempts
     WHERE research_run_attempt_id = NEW.research_run_attempt_id FOR SHARE;
    IF actual_spec <> expected_spec THEN RAISE EXCEPTION 'M0b artifact attempt/candidate mismatch'; END IF;
    IF epoch_status <> 'RUNNING' OR campaign_status <> 'RUNNING'
       OR holdout_revealed_at IS NOT NULL OR closed_at IS NOT NULL
       OR candidate_status <> 'RUNNING' THEN
        RAISE EXCEPTION 'M0b artifact links require an active unrevealed search epoch';
    END IF;
    SELECT artifact_type, sha256, byte_size, metadata INTO STRICT artifact_record
      FROM systematic_fx.artifacts WHERE artifact_id = NEW.artifact_id FOR SHARE;
    IF artifact_record.sha256 IS DISTINCT FROM NEW.artifact_sha256
       OR artifact_record.byte_size IS DISTINCT FROM NEW.artifact_byte_size THEN
        RAISE EXCEPTION 'M0b linked artifact byte identity mismatch';
    END IF;
    IF NEW.artifact_role = 'RESULT' AND (
        attempt_status <> 'SUCCEEDED'
        OR attempt_result_artifact_id IS DISTINCT FROM NEW.artifact_id
        OR artifact_record.artifact_type <> 'M0B_RESULT'
        OR artifact_record.metadata IS DISTINCT FROM jsonb_build_object(
            'identity_schema', 'systematic_fx.m0b.result.v1',
            'epoch_sha256', epoch_sha256,
            'm0b_epoch_id', m0b_epoch_id,
            'candidate_sha256', candidate_sha256,
            'm0b_candidate_id', NEW.m0b_candidate_id,
            'research_run_attempt_id', NEW.research_run_attempt_id,
            'result_sha256', NEW.artifact_sha256,
            'admission_rules_sha256', admission_rules_sha256)) THEN
        RAISE EXCEPTION 'M0b RESULT link must bind the exact successful attempt result';
    ELSIF NEW.artifact_role = 'FAILURE' AND attempt_status <> 'FAILED' THEN
        RAISE EXCEPTION 'M0b FAILURE link requires a failed attempt';
    ELSIF NEW.artifact_role = 'DETAIL'
          AND candidate_status <> 'RUNNING' THEN
        RAISE EXCEPTION 'M0b detail artifacts may be linked only while candidate is running';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER m0b_artifact_links_validate_insert
BEFORE INSERT ON systematic_fx.m0b_artifact_links
FOR EACH ROW EXECUTE FUNCTION systematic_fx.validate_m0b_artifact_link_insert();

CREATE FUNCTION systematic_fx.validate_m0b_candidate_terminal()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE attempt_status text; result_artifact bigint; result_link_count integer;
        attempt_id bigint; attempt_started_at timestamptz; attempt_finished_at timestamptz;
        attempt_result_summary jsonb;
        epoch_status text; campaign_status text; holdout_revealed_at timestamptz;
        closed_at timestamptz;
BEGIN
    IF NEW.status NOT IN ('SCREENED_OUT', 'REGISTERED') THEN RETURN NEW; END IF;
    IF NEW.status = 'REGISTERED' AND NEW.candidate_kind <> 'REAL' THEN
        RAISE EXCEPTION 'M0b NULL controls cannot be REGISTERED';
    END IF;
    SELECT epoch.status, campaign.status, campaign.holdout_revealed_at, campaign.closed_at
      INTO STRICT epoch_status, campaign_status, holdout_revealed_at, closed_at
      FROM systematic_fx.m0b_epochs AS epoch
      JOIN systematic_fx.campaigns AS campaign USING (campaign_id)
     WHERE epoch.m0b_epoch_id = NEW.m0b_epoch_id FOR SHARE;
    IF epoch_status <> 'RUNNING' OR campaign_status <> 'RUNNING'
       OR holdout_revealed_at IS NOT NULL OR closed_at IS NOT NULL THEN
        RAISE EXCEPTION 'M0b candidate terminalization requires active unrevealed search';
    END IF;
    SELECT research_run_attempt_id, status, result_artifact_id, started_at, finished_at,
           result_summary
      INTO STRICT attempt_id, attempt_status, result_artifact,
           attempt_started_at, attempt_finished_at, attempt_result_summary
      FROM systematic_fx.research_run_attempts
     WHERE research_run_spec_id = NEW.research_run_spec_id
     ORDER BY attempt_number DESC LIMIT 1 FOR SHARE;
    IF NEW.status IN ('SCREENED_OUT', 'REGISTERED') THEN
        IF attempt_status <> 'SUCCEEDED' OR result_artifact IS NULL
           OR attempt_started_at IS NULL OR attempt_finished_at IS NULL
           OR attempt_result_summary ->> 'classification' IS DISTINCT FROM NEW.status THEN
            RAISE EXCEPTION 'M0b economic terminal state requires SUCCEEDED attempt';
        END IF;
        SELECT count(*)::integer INTO result_link_count
          FROM systematic_fx.m0b_artifact_links l
         WHERE l.m0b_candidate_id = NEW.m0b_candidate_id AND l.artifact_role = 'RESULT'
           AND l.artifact_id = result_artifact
           AND l.research_run_attempt_id = attempt_id;
        IF result_link_count <> 1 THEN RAISE EXCEPTION 'M0b result artifact link missing'; END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER m0b_candidates_terminal_pair
AFTER INSERT OR UPDATE ON systematic_fx.m0b_candidates
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION systematic_fx.validate_m0b_candidate_terminal();

CREATE FUNCTION systematic_fx.validate_m0b_candidate_failure()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE latest_status text; active_attempt_count integer; attempt_count integer;
BEGIN
    IF NEW.status NOT IN ('FAILED', 'CRASHED') THEN RETURN NEW; END IF;
    SELECT count(*)::integer,
           count(*) FILTER (WHERE status IN ('QUEUED', 'RUNNING'))::integer
      INTO attempt_count, active_attempt_count
      FROM systematic_fx.research_run_attempts
     WHERE research_run_spec_id = NEW.research_run_spec_id;
    SELECT status INTO latest_status
      FROM systematic_fx.research_run_attempts
     WHERE research_run_spec_id = NEW.research_run_spec_id
     ORDER BY attempt_number DESC LIMIT 1;
    IF active_attempt_count <> 0
       OR (attempt_count > 0 AND latest_status <> 'FAILED') THEN
        RAISE EXCEPTION 'M0b failed/crashed candidate requires no active attempt and latest failure';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER m0b_candidates_failure_pair
AFTER INSERT OR UPDATE ON systematic_fx.m0b_candidates
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION systematic_fx.validate_m0b_candidate_failure();

CREATE FUNCTION systematic_fx.protect_m0b_attempt_lifecycle()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    candidate_record record;
    result_artifact_record record;
    attempt_count integer;
    governed_epoch_id bigint;
BEGIN
    SELECT candidate.status AS candidate_status, epoch.status AS epoch_status,
           epoch.max_attempts_per_candidate,
           epoch.epoch_sha256, candidate.candidate_sha256,
           systematic_fx.canonical_jsonb_sha256(
               epoch.canonical_epoch -> 'admission_rules') AS admission_rules_sha256,
           campaign.status AS campaign_status, campaign.holdout_revealed_at,
           campaign.closed_at
      INTO candidate_record
      FROM systematic_fx.m0b_candidates AS candidate
      JOIN systematic_fx.m0b_epochs AS epoch USING (m0b_epoch_id)
      JOIN systematic_fx.campaigns AS campaign USING (campaign_id)
     WHERE candidate.research_run_spec_id = NEW.research_run_spec_id
     FOR SHARE;
    IF NOT FOUND THEN
        SELECT epoch.m0b_epoch_id
          INTO governed_epoch_id
          FROM systematic_fx.research_run_specs AS run_spec
          JOIN systematic_fx.m0b_epochs AS epoch
            ON epoch.campaign_id = run_spec.campaign_id
         WHERE run_spec.research_run_spec_id = NEW.research_run_spec_id;
        IF governed_epoch_id IS NOT NULL THEN
            RAISE EXCEPTION
                'M0b campaign RunSpecs require a budgeted candidate before attempts';
        END IF;
        RETURN NEW;
    END IF;
    IF candidate_record.candidate_status IN
           ('SCREENED_OUT', 'REGISTERED', 'FAILED', 'CRASHED') THEN
        RAISE EXCEPTION 'terminal M0b candidates cannot receive attempt mutations';
    END IF;
    IF candidate_record.epoch_status <> 'RUNNING'
       OR candidate_record.campaign_status <> 'RUNNING'
       OR candidate_record.holdout_revealed_at IS NOT NULL
       OR candidate_record.closed_at IS NOT NULL THEN
        RAISE EXCEPTION 'M0b attempts require an active unrevealed search epoch';
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.status IN ('QUEUED', 'RUNNING')
       AND NEW.status IN ('REJECTED', 'CANCELLED', 'SKIPPED_DUPLICATE') THEN
        RAISE EXCEPTION 'M0b attempts terminate only SUCCEEDED or FAILED';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF (OLD.status, NEW.status) NOT IN (
               ('QUEUED', 'RUNNING'), ('QUEUED', 'FAILED'),
               ('RUNNING', 'SUCCEEDED'), ('RUNNING', 'FAILED')) THEN
            RAISE EXCEPTION 'invalid M0b attempt transition';
        END IF;
        IF ROW(NEW.job_id, NEW.reused_attempt_id)
             IS DISTINCT FROM ROW(OLD.job_id, OLD.reused_attempt_id)
           OR (OLD.started_at IS NOT NULL AND NEW.started_at IS DISTINCT FROM OLD.started_at)
           OR (OLD.result_artifact_id IS NOT NULL
               AND NEW.result_artifact_id IS DISTINCT FROM OLD.result_artifact_id)
           OR (OLD.trade_ledger_artifact_id IS NOT NULL
               AND NEW.trade_ledger_artifact_id
                    IS DISTINCT FROM OLD.trade_ledger_artifact_id) THEN
            RAISE EXCEPTION 'M0b attempt lifecycle identity is immutable';
        END IF;
        IF NEW.status = 'RUNNING' AND (NEW.started_at IS NULL
                                      OR NEW.finished_at IS NOT NULL
                                      OR NEW.error_message IS NOT NULL
                                      OR NEW.result_artifact_id IS NOT NULL) THEN
            RAISE EXCEPTION 'M0b RUNNING attempt shape is invalid';
        ELSIF NEW.status = 'SUCCEEDED' AND (
            NEW.started_at IS NULL OR NEW.finished_at IS NULL
            OR NEW.result_artifact_id IS NULL OR NEW.error_message IS NOT NULL
            OR NEW.trade_ledger_artifact_id IS NOT NULL) THEN
            RAISE EXCEPTION 'M0b SUCCEEDED attempt shape is invalid';
        ELSIF NEW.status = 'FAILED' AND (
            NEW.finished_at IS NULL OR btrim(COALESCE(NEW.error_message, '')) = ''
            OR NEW.result_artifact_id IS NOT NULL
            OR NEW.trade_ledger_artifact_id IS NOT NULL
            OR NEW.result_summary <> '{}'::jsonb) THEN
            RAISE EXCEPTION 'M0b FAILED attempt shape is invalid';
        END IF;
        IF NEW.status = 'SUCCEEDED' THEN
            SELECT artifact_type, sha256 INTO STRICT result_artifact_record
              FROM systematic_fx.artifacts
             WHERE artifact_id = NEW.result_artifact_id FOR SHARE;
            IF result_artifact_record.artifact_type <> 'M0B_RESULT'
               OR NEW.result_summary IS DISTINCT FROM jsonb_build_object(
                    'identity_schema', 'systematic_fx.m0b.result_summary.v1',
                    'epoch_sha256', candidate_record.epoch_sha256,
                    'candidate_sha256', candidate_record.candidate_sha256,
                    'result_artifact_id', NEW.result_artifact_id,
                    'result_sha256', result_artifact_record.sha256,
                    'data_role', 'SEARCH',
                    'classification', NEW.result_summary ->> 'classification',
                    'admission_rules_sha256',
                        candidate_record.admission_rules_sha256)
               OR NEW.result_summary ->> 'classification' IS NULL
               OR NEW.result_summary ->> 'classification'
                    NOT IN ('SCREENED_OUT', 'REGISTERED') THEN
                RAISE EXCEPTION 'M0b SUCCEEDED attempt result summary is not exact search evidence';
            END IF;
        END IF;
    END IF;
    IF TG_OP = 'INSERT' THEN
        SELECT count(*)::integer INTO attempt_count
          FROM systematic_fx.research_run_attempts
         WHERE research_run_spec_id = NEW.research_run_spec_id;
        IF NEW.attempt_number <> attempt_count + 1
           OR NEW.attempt_number > candidate_record.max_attempts_per_candidate
           OR NEW.status <> 'QUEUED'
           OR NEW.reused_attempt_id IS NOT NULL
           OR NEW.started_at IS NOT NULL OR NEW.finished_at IS NOT NULL
           OR NEW.error_message IS NOT NULL OR NEW.result_artifact_id IS NOT NULL
           OR NEW.trade_ledger_artifact_id IS NOT NULL
           OR NEW.result_summary <> '{}'::jsonb
           OR EXISTS (
               SELECT 1 FROM systematic_fx.research_run_attempts
                WHERE research_run_spec_id = NEW.research_run_spec_id
                  AND status IN ('QUEUED', 'RUNNING', 'SUCCEEDED')
           ) THEN
            RAISE EXCEPTION 'M0b retry requires prior terminal failures and one pristine queue';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER research_run_attempts_protect_m0b_lifecycle
BEFORE INSERT OR UPDATE ON systematic_fx.research_run_attempts
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_m0b_attempt_lifecycle();

CREATE FUNCTION systematic_fx.require_m0b_success_candidate_pair()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE candidate_record record; link_count integer;
BEGIN
    IF NEW.status <> 'SUCCEEDED' THEN RETURN NEW; END IF;
    SELECT candidate.m0b_candidate_id, candidate.status
      INTO candidate_record
      FROM systematic_fx.m0b_candidates AS candidate
     WHERE candidate.research_run_spec_id = NEW.research_run_spec_id
     FOR SHARE;
    IF NOT FOUND THEN RETURN NEW; END IF;
    IF candidate_record.status NOT IN ('SCREENED_OUT', 'REGISTERED') THEN
        RAISE EXCEPTION 'M0b SUCCEEDED attempt requires atomic terminal candidate';
    END IF;
    SELECT count(*)::integer INTO link_count
      FROM systematic_fx.m0b_artifact_links AS link
     WHERE link.m0b_candidate_id = candidate_record.m0b_candidate_id
       AND link.research_run_attempt_id = NEW.research_run_attempt_id
       AND link.artifact_role = 'RESULT'
       AND link.artifact_id = NEW.result_artifact_id;
    IF link_count <> 1 THEN
        RAISE EXCEPTION 'M0b SUCCEEDED attempt requires atomic exact result link';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER research_run_attempts_require_m0b_candidate_pair
AFTER INSERT OR UPDATE ON systematic_fx.research_run_attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION systematic_fx.require_m0b_success_candidate_pair();

CREATE FUNCTION systematic_fx.validate_m0b_epoch_terminal()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE real_count integer; null_count integer; nonterminal_count integer;
        candidate_count integer; failed_candidate_count integer;
        active_attempt_count integer; unpaired_attempt_count integer;
        run_spec_count integer; missing_null_control_count integer;
        campaign_status text; holdout_revealed_at timestamptz; closed_at timestamptz;
BEGIN
    IF NEW.status NOT IN ('COMPLETED', 'FAILED') THEN RETURN NEW; END IF;
    SELECT campaign.status, campaign.holdout_revealed_at, campaign.closed_at
      INTO STRICT campaign_status, holdout_revealed_at, closed_at
      FROM systematic_fx.campaigns AS campaign
     WHERE campaign.campaign_id = NEW.campaign_id FOR SHARE;
    IF campaign_status <> 'RUNNING' OR holdout_revealed_at IS NOT NULL
       OR closed_at IS NOT NULL THEN
        RAISE EXCEPTION 'M0b terminalization requires active unrevealed search campaign';
    END IF;
    SELECT count(*) FILTER (WHERE candidate_kind = 'REAL')::integer,
           count(*) FILTER (WHERE candidate_kind = 'NULL')::integer,
           count(*) FILTER (WHERE status NOT IN
               ('SCREENED_OUT', 'REGISTERED', 'FAILED', 'CRASHED'))::integer,
           count(*)::integer,
           count(*) FILTER (WHERE status IN ('FAILED', 'CRASHED'))::integer
      INTO real_count, null_count, nonterminal_count, candidate_count,
           failed_candidate_count
      FROM systematic_fx.m0b_candidates WHERE m0b_epoch_id = NEW.m0b_epoch_id;
    SELECT count(*)::integer INTO run_spec_count
      FROM systematic_fx.research_run_specs
     WHERE campaign_id = NEW.campaign_id;
    SELECT count(*)::integer INTO missing_null_control_count
      FROM jsonb_array_elements_text(
               NEW.canonical_epoch -> 'null_controls') AS declared(control)
     WHERE NOT EXISTS (
         SELECT 1
           FROM systematic_fx.m0b_candidates AS candidate
          WHERE candidate.m0b_epoch_id = NEW.m0b_epoch_id
            AND candidate.candidate_kind = 'NULL'
            AND candidate.canonical_candidate #>> '{null_control}' = declared.control
     );
    SELECT count(*) FILTER (WHERE attempt.status IN ('QUEUED', 'RUNNING'))::integer,
           count(*) FILTER (WHERE candidate.m0b_candidate_id IS NULL)::integer
      INTO active_attempt_count, unpaired_attempt_count
      FROM systematic_fx.research_run_attempts AS attempt
      JOIN systematic_fx.research_run_specs AS run_spec USING (research_run_spec_id)
      LEFT JOIN systematic_fx.m0b_candidates AS candidate USING (research_run_spec_id)
     WHERE run_spec.campaign_id = NEW.campaign_id;
    IF active_attempt_count <> 0 OR unpaired_attempt_count <> 0
       OR run_spec_count <> candidate_count THEN
        RAISE EXCEPTION
            'M0b terminal epoch cannot retain active or unpaired attempts or RunSpecs';
    END IF;
    IF NEW.status = 'COMPLETED' AND (
       real_count <> NEW.real_candidate_budget OR null_count <> NEW.null_candidate_budget
       OR nonterminal_count <> 0 OR failed_candidate_count <> 0) THEN
        RAISE EXCEPTION 'M0b COMPLETED requires exact spent budgets and terminal candidates';
    ELSIF NEW.status = 'COMPLETED' AND missing_null_control_count <> 0 THEN
        RAISE EXCEPTION 'M0b COMPLETED requires full declared null-control coverage';
    ELSIF NEW.status = 'FAILED' AND (
       candidate_count > 0 AND nonterminal_count <> 0) THEN
        RAISE EXCEPTION 'M0b FAILED requires every registered candidate terminal';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER m0b_epochs_terminal_cardinality
AFTER INSERT OR UPDATE ON systematic_fx.m0b_epochs
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION systematic_fx.validate_m0b_epoch_terminal();

CREATE FUNCTION systematic_fx.require_m0b_run_spec_candidate_pair()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE epoch_id bigint; candidate_count integer;
BEGIN
    SELECT m0b_epoch_id INTO epoch_id
      FROM systematic_fx.m0b_epochs WHERE campaign_id = NEW.campaign_id;
    IF epoch_id IS NULL THEN RETURN NEW; END IF;
    SELECT count(*)::integer INTO candidate_count
      FROM systematic_fx.m0b_candidates
     WHERE m0b_epoch_id = epoch_id
       AND research_run_spec_id = NEW.research_run_spec_id;
    IF candidate_count <> 1 THEN
        RAISE EXCEPTION 'M0b RunSpec and candidate must register atomically one-to-one';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER research_run_specs_require_m0b_candidate_pair
AFTER INSERT ON systematic_fx.research_run_specs
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION systematic_fx.require_m0b_run_spec_candidate_pair();

CREATE FUNCTION systematic_fx.protect_m0b_run_spec_lineage()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE epoch_record record; campaign_key text; experiment_key text;
        parent_fingerprint text; locked_campaign_id bigint;
BEGIN
    -- Epoch bootstrap and RunSpec registration share this campaign lock.  Without
    -- it, a PREPARED epoch and an unpaired generic RunSpec can cross-commit after
    -- each independently observes an empty namespace.
    SELECT campaign_id INTO STRICT locked_campaign_id
      FROM systematic_fx.campaigns
     WHERE campaign_id = NEW.campaign_id
     FOR UPDATE;
    SELECT * INTO epoch_record
      FROM systematic_fx.m0b_epochs
     WHERE campaign_id = NEW.campaign_id FOR SHARE;
    IF NOT FOUND THEN RETURN NEW; END IF;
    SELECT campaign.campaign_key, experiment.experiment_key
      INTO STRICT campaign_key, experiment_key
      FROM systematic_fx.campaigns AS campaign
      JOIN systematic_fx.experiments AS experiment
        ON experiment.campaign_id = campaign.campaign_id
     WHERE campaign.campaign_id = NEW.campaign_id
       AND experiment.experiment_id = NEW.experiment_id;
    IF NEW.parent_run_spec_id IS NOT NULL THEN
        SELECT run_fingerprint INTO STRICT parent_fingerprint
          FROM systematic_fx.research_run_specs
         WHERE research_run_spec_id = NEW.parent_run_spec_id FOR SHARE;
    END IF;
    IF epoch_record.status NOT IN ('PREPARED', 'RUNNING')
       OR NEW.canonicalization_schema <> 'systematic_fx.research_run_spec.v2'
       OR NEW.canonicalization_version <> 2
       OR systematic_fx.m0b_json_has_forbidden_reference(NEW.canonical_spec)
       OR (SELECT array_agg(key ORDER BY key)
             FROM jsonb_object_keys(NEW.canonical_spec) AS key)
            IS DISTINCT FROM ARRAY[
                'artifact_schema', 'barrier_policy', 'campaign_id', 'code_commit',
                'code_snapshot_sha256', 'cost', 'dependency_lock_sha256',
                'direction', 'eligible_calendar', 'engine_version', 'entry_policy',
                'execution', 'experiment_id', 'feature', 'outcome', 'parameters',
                'random_seed', 'run_kind', 'runtime_environment', 'schema_version',
                'signal_policy', 'source_manifest_hashes', 'split',
                'terminal_policy']::text[]
       OR systematic_fx.canonical_jsonb_sha256(NEW.canonical_spec)
            IS DISTINCT FROM NEW.run_fingerprint
       OR NEW.run_kind <> 'SCREEN'
       OR NEW.experiment_id IS NULL
       OR NEW.canonical_spec #>> '{artifact_schema}'
            IS DISTINCT FROM 'systematic_fx.research_run_spec.v2'
       OR (NEW.canonical_spec #>> '{schema_version}')::integer IS DISTINCT FROM 2
       OR NEW.canonical_spec #>> '{campaign_id}' IS DISTINCT FROM campaign_key
       OR NEW.canonical_spec #>> '{experiment_id}' IS DISTINCT FROM experiment_key
       OR NEW.canonical_spec #>> '{run_kind}' IS DISTINCT FROM NEW.run_kind
       OR NEW.canonical_spec #>> '{engine_version}' IS DISTINCT FROM NEW.engine_version
       OR NEW.canonical_spec -> 'source_manifest_hashes'
            IS DISTINCT FROM NEW.source_manifest_hashes
       OR NEW.canonical_spec -> 'eligible_calendar' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.eligible_calendar_version,
            'sha256', NEW.eligible_calendar_sha256)
       OR NEW.canonical_spec -> 'split' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.split_version, 'sha256', NEW.split_sha256)
       OR NEW.canonical_spec -> 'feature' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.feature_version, 'sha256', NEW.feature_sha256)
       OR NEW.canonical_spec -> 'outcome' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.outcome_version, 'sha256', NEW.outcome_sha256)
       OR NEW.canonical_spec -> 'cost' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.cost_version, 'sha256', NEW.cost_sha256)
       OR NEW.canonical_spec -> 'execution' IS DISTINCT FROM jsonb_build_object(
            'version', NEW.execution_version, 'sha256', NEW.execution_sha256)
       OR NEW.canonical_spec #>> '{code_commit}' IS DISTINCT FROM NEW.code_commit
       OR NEW.canonical_spec #>> '{code_snapshot_sha256}'
            IS DISTINCT FROM NEW.code_snapshot_sha256
       OR NEW.canonical_spec #>> '{dependency_lock_sha256}'
            IS DISTINCT FROM NEW.dependency_lock_sha256
       OR (NEW.canonical_spec #>> '{random_seed}')::numeric
            IS DISTINCT FROM NEW.deterministic_seed
       OR NEW.canonical_spec #>> '{direction}' IS DISTINCT FROM NEW.direction
       OR NEW.canonical_spec #>> '{parameters,data_role}' IS DISTINCT FROM 'SEARCH'
       OR NEW.canonical_spec #>> '{parameters,split_role}' IS DISTINCT FROM 'DISCOVERY'
       OR NEW.canonical_spec #>> '{parameters,m0b_epoch_sha256}'
            IS DISTINCT FROM epoch_record.epoch_sha256
       OR (SELECT array_agg(key ORDER BY key)
             FROM jsonb_object_keys(NEW.canonical_spec -> 'parameters') AS key)
            IS DISTINCT FROM
            (CASE WHEN NEW.parent_run_spec_id IS NULL THEN ARRAY[
                'data_role', 'm0b_candidate_sha256',
                'm0b_contract_reference_sha256', 'm0b_dataset_sha256',
                'm0b_epoch_sha256', 'split_role']::text[]
             ELSE ARRAY[
                'data_role', 'm0b_candidate_sha256',
                'm0b_contract_reference_sha256', 'm0b_dataset_sha256',
                'm0b_epoch_sha256', 'parent_run_fingerprint',
                'split_role']::text[] END)
       OR NEW.canonical_spec #>> '{parameters,m0b_dataset_sha256}'
            IS DISTINCT FROM epoch_record.dataset_sha256
       OR NEW.canonical_spec #>> '{parameters,m0b_contract_reference_sha256}'
            IS DISTINCT FROM epoch_record.contract_reference_sha256
       OR NEW.canonical_spec #>> '{parameters,m0b_candidate_sha256}' IS NULL
       OR (NEW.parent_run_spec_id IS NULL AND
           NEW.canonical_spec -> 'parameters' ? 'parent_run_fingerprint')
       OR (NEW.parent_run_spec_id IS NOT NULL AND
           NEW.canonical_spec #>> '{parameters,parent_run_fingerprint}'
                IS DISTINCT FROM parent_fingerprint)
       OR NEW.source_manifest_hashes
            IS DISTINCT FROM jsonb_build_object('dataset', epoch_record.dataset_sha256)
       OR NEW.canonical_spec -> 'entry_policy' IS DISTINCT FROM jsonb_build_object(
            'latency', lower(epoch_record.canonical_epoch #>>
                '{execution_assumptions,entry_latency}'))
       OR NEW.canonical_spec -> 'barrier_policy'
            IS DISTINCT FROM '{"kind":"volatility_normalized"}'::jsonb
       OR NEW.canonical_spec -> 'terminal_policy' IS DISTINCT FROM jsonb_build_object(
            'session_policy', epoch_record.canonical_epoch #>> '{session_policy}')
       OR NEW.canonical_spec -> 'signal_policy' IS DISTINCT FROM jsonb_build_object(
            'family', NEW.canonical_spec #>> '{signal_policy,family}')
       OR jsonb_typeof(NEW.canonical_spec -> 'runtime_environment')
            IS DISTINCT FROM 'object'
       OR NEW.canonical_spec -> 'runtime_environment' = '{}'::jsonb
       OR NEW.eligible_calendar_version IS DISTINCT FROM epoch_record.calendar_version
       OR NEW.eligible_calendar_sha256 IS DISTINCT FROM epoch_record.calendar_sha256
       OR NEW.split_version IS DISTINCT FROM epoch_record.split_version
       OR NEW.split_sha256 IS DISTINCT FROM epoch_record.split_sha256
       OR NEW.feature_version IS DISTINCT FROM epoch_record.feature_version
       OR NEW.feature_sha256 IS DISTINCT FROM epoch_record.feature_sha256
       OR NEW.outcome_version IS DISTINCT FROM epoch_record.label_version
       OR NEW.outcome_sha256 IS DISTINCT FROM epoch_record.label_sha256
       OR NEW.cost_version IS DISTINCT FROM epoch_record.cost_version
       OR NEW.cost_sha256 IS DISTINCT FROM epoch_record.cost_sha256
       OR NEW.execution_version IS DISTINCT FROM epoch_record.execution_version
       OR NEW.execution_sha256 IS DISTINCT FROM epoch_record.execution_sha256
       OR NEW.engine_version IS DISTINCT FROM epoch_record.engine_version
       OR NEW.code_commit IS DISTINCT FROM epoch_record.code_commit
       OR NEW.code_snapshot_sha256 IS DISTINCT FROM epoch_record.code_snapshot_sha256
       OR NEW.dependency_lock_sha256 IS DISTINCT FROM epoch_record.dependency_lock_sha256 THEN
        RAISE EXCEPTION 'M0b campaign accepts only frozen search-data SCREEN RunSpecs';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER research_run_specs_restrict_m0b_epoch
BEFORE INSERT ON systematic_fx.research_run_specs
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_m0b_run_spec_lineage();

COMMENT ON TABLE systematic_fx.m0b_epochs IS
    'Immutable finite-budget M0b search-data epoch; daemon authority stops at REGISTER.';
COMMENT ON TABLE systematic_fx.m0b_candidates IS
    'Finite REAL/NULL search candidates bound to generic RunSpec/attempt authority; no holdout roles.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (29, 'm0b_governed_control_plane', :'migration_checksum');

COMMIT;
