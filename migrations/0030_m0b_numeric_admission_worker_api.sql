BEGIN;

-- M0b admission remains an exploratory SEARCH-data screen.  These predicates
-- are immutable epoch input, not a paper/live promotion policy.
CREATE FUNCTION systematic_fx.m0b_numeric_admission_rules_valid(rules jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
SET search_path = pg_catalog AS $$
    SELECT jsonb_typeof(rules) = 'object'
       AND length(rules::text) <= 4096
       AND (SELECT array_agg(key ORDER BY key) FROM jsonb_object_keys(rules) key)
           = ARRAY[
               'contract_version', 'max_stressed_cost_ev_floor_ticks',
               'maximum_authority', 'min_active_days', 'min_flat_trades',
               'min_positive_search_folds', 'min_raw_events',
               'min_sequential_trades', 'min_tp_probability_ppm',
               'require_positive_net_ev']::text[]
       AND rules #>> '{contract_version}' = 'm0b_numeric_admission_v1'
       AND rules #>> '{maximum_authority}' = 'REGISTER'
       AND jsonb_typeof(rules -> 'require_positive_net_ev') = 'boolean'
       AND jsonb_typeof(rules -> 'max_stressed_cost_ev_floor_ticks') = 'number'
       AND jsonb_typeof(rules -> 'min_raw_events') = 'number'
       AND jsonb_typeof(rules -> 'min_flat_trades') = 'number'
       AND jsonb_typeof(rules -> 'min_sequential_trades') = 'number'
       AND jsonb_typeof(rules -> 'min_active_days') = 'number'
       AND jsonb_typeof(rules -> 'min_tp_probability_ppm') = 'number'
       AND jsonb_typeof(rules -> 'min_positive_search_folds') = 'number'
       AND rules ->> 'max_stressed_cost_ev_floor_ticks' ~ '^-?(0|[1-9][0-9]*)$'
       AND rules ->> 'min_raw_events' ~ '^(0|[1-9][0-9]*)$'
       AND rules ->> 'min_flat_trades' ~ '^(0|[1-9][0-9]*)$'
       AND rules ->> 'min_sequential_trades' ~ '^(0|[1-9][0-9]*)$'
       AND rules ->> 'min_active_days' ~ '^(0|[1-9][0-9]*)$'
       AND rules ->> 'min_tp_probability_ppm' ~ '^(0|[1-9][0-9]*)$'
       AND rules ->> 'min_positive_search_folds' ~ '^(0|[1-9][0-9]*)$'
       AND length(rules ->> 'max_stressed_cost_ev_floor_ticks') <= 10
       AND length(rules ->> 'min_raw_events') <= 10
       AND length(rules ->> 'min_flat_trades') <= 10
       AND length(rules ->> 'min_sequential_trades') <= 10
       AND length(rules ->> 'min_active_days') <= 10
       AND length(rules ->> 'min_tp_probability_ppm') <= 7
       AND length(rules ->> 'min_positive_search_folds') <= 10
       AND (rules ->> 'min_tp_probability_ppm')::numeric <= 1000000
       AND (rules ->> 'min_positive_search_folds')::numeric <= 2147483647
       AND (rules ->> 'min_tp_probability_ppm')::integer BETWEEN 0 AND 1000000;
$$;

CREATE FUNCTION systematic_fx.m0b_numeric_metrics_valid(metrics jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
SET search_path = pg_catalog AS $$
    SELECT jsonb_typeof(metrics) = 'object'
       AND length(metrics::text) <= 4096
       AND (SELECT array_agg(key ORDER BY key) FROM jsonb_object_keys(metrics) key)
           = ARRAY[
               'active_days', 'flat_trades', 'net_pnl_ticks',
               'positive_search_folds', 'raw_events', 'sequential_trades',
               'stressed_net_pnl_ticks', 'tp_probability_ppm']::text[]
       AND jsonb_typeof(metrics -> 'raw_events') = 'number'
       AND jsonb_typeof(metrics -> 'flat_trades') = 'number'
       AND jsonb_typeof(metrics -> 'sequential_trades') = 'number'
       AND jsonb_typeof(metrics -> 'active_days') = 'number'
       AND jsonb_typeof(metrics -> 'tp_probability_ppm') = 'number'
       AND jsonb_typeof(metrics -> 'positive_search_folds') = 'number'
       AND jsonb_typeof(metrics -> 'net_pnl_ticks') = 'number'
       AND jsonb_typeof(metrics -> 'stressed_net_pnl_ticks') = 'number'
       AND metrics ->> 'raw_events' ~ '^(0|[1-9][0-9]*)$'
       AND metrics ->> 'flat_trades' ~ '^(0|[1-9][0-9]*)$'
       AND metrics ->> 'sequential_trades' ~ '^(0|[1-9][0-9]*)$'
       AND metrics ->> 'active_days' ~ '^(0|[1-9][0-9]*)$'
       AND metrics ->> 'tp_probability_ppm' ~ '^(0|[1-9][0-9]*)$'
       AND metrics ->> 'positive_search_folds' ~ '^(0|[1-9][0-9]*)$'
       AND metrics ->> 'net_pnl_ticks' ~ '^-?(0|[1-9][0-9]*)$'
       AND metrics ->> 'stressed_net_pnl_ticks' ~ '^-?(0|[1-9][0-9]*)$'
       AND length(metrics ->> 'raw_events') <= 19
       AND length(metrics ->> 'flat_trades') <= 19
       AND length(metrics ->> 'sequential_trades') <= 19
       AND length(metrics ->> 'active_days') <= 10
       AND length(metrics ->> 'tp_probability_ppm') <= 7
       AND length(metrics ->> 'positive_search_folds') <= 10
       AND length(metrics ->> 'net_pnl_ticks') <= 19
       AND length(metrics ->> 'stressed_net_pnl_ticks') <= 19
       AND (metrics ->> 'raw_events')::numeric <= 9000000000000000000
       AND (metrics ->> 'flat_trades')::numeric <= 9000000000000000000
       AND (metrics ->> 'sequential_trades')::numeric <= 9000000000000000000
       AND (metrics ->> 'active_days')::numeric <= 2147483647
       AND (metrics ->> 'positive_search_folds')::numeric <= 2147483647
       AND abs((metrics ->> 'net_pnl_ticks')::numeric) <= 9000000000000000000
       AND abs((metrics ->> 'stressed_net_pnl_ticks')::numeric)
           <= 9000000000000000000
       AND (metrics ->> 'tp_probability_ppm')::integer BETWEEN 0 AND 1000000;
$$;

CREATE FUNCTION systematic_fx.m0b_numeric_metrics_admitted(rules jsonb, metrics jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
SET search_path = pg_catalog AS $$
    SELECT systematic_fx.m0b_numeric_admission_rules_valid(rules)
       AND systematic_fx.m0b_numeric_metrics_valid(metrics)
       AND (metrics ->> 'raw_events')::bigint >= (rules ->> 'min_raw_events')::bigint
       AND (metrics ->> 'flat_trades')::bigint >= (rules ->> 'min_flat_trades')::bigint
       AND (metrics ->> 'sequential_trades')::bigint
           >= (rules ->> 'min_sequential_trades')::bigint
       AND (metrics ->> 'active_days')::bigint >= (rules ->> 'min_active_days')::bigint
       AND (metrics ->> 'tp_probability_ppm')::integer
           >= (rules ->> 'min_tp_probability_ppm')::integer
       AND (metrics ->> 'positive_search_folds')::integer
           >= (rules ->> 'min_positive_search_folds')::integer
       AND (metrics ->> 'sequential_trades')::bigint > 0
       AND ((rules ->> 'require_positive_net_ev')::boolean IS FALSE
            OR (metrics ->> 'net_pnl_ticks')::bigint > 0)
       AND (metrics ->> 'stressed_net_pnl_ticks')::numeric
           >= (rules ->> 'max_stressed_cost_ev_floor_ticks')::numeric
              * (metrics ->> 'sequential_trades')::numeric;
$$;

-- Checkpoint state may name only the worker's immutable, content-addressed
-- leaf artifacts.  The generic 0029 reference guard deliberately rejects
-- every path/URI key, so this validator fixes the complete state shape first
-- and admits only the two exact leaf grammars used for deterministic resume.
CREATE FUNCTION systematic_fx.m0b_worker_checkpoint_state_valid(
    state jsonb,
    evaluation_policy jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
SET search_path = pg_catalog AS $$
DECLARE
    field_name text;
    shard jsonb;
    shard_index integer := 0;
    expected_store_shard numeric := 1;
    shard_sha256 text;
    result_identity jsonb;
    sequential_count numeric;
    raw_count numeric;
    matching_count numeric;
    signal_index numeric;
    shard_row_total numeric := 0;
    fold_trade_total numeric;
    fold_pnl_total numeric;
    stress_cost numeric;
BEGIN
    IF jsonb_typeof(state) IS DISTINCT FROM 'object'
       OR pg_column_size(state) > 1048576
       OR (SELECT array_agg(key ORDER BY key)
             FROM jsonb_object_keys(state) AS key)
            IS DISTINCT FROM ARRAY[
                'accepted_tp_count', 'active_session_ids', 'complete',
                'fold_net_pnl_ticks', 'fold_trade_counts',
                'ineligible_signal_count', 'matching_label_count',
                'missing_label_count', 'next_available_ts_ns',
                'next_shard_ordinal', 'next_signal_index',
                'overlap_signal_count', 'raw_event_count',
                'raw_net_pnl_ticks', 'raw_tp_count', 'result_artifact',
                'sequential_net_pnl_ticks',
                'sequential_stressed_net_pnl_ticks',
                'sequential_trade_count', 'state_schema', 'trade_shards',
                'work_spec_sha256']::text[]
       OR state #>> '{state_schema}'
            IS DISTINCT FROM 'systematic_fx.m0b_worker_state.v1'
       OR (state #>> '{work_spec_sha256}' ~ '^[0-9a-f]{64}$') IS NOT TRUE
       OR jsonb_typeof(state -> 'complete') IS DISTINCT FROM 'boolean'
       OR jsonb_typeof(state -> 'active_session_ids') IS DISTINCT FROM 'array'
       OR jsonb_typeof(state -> 'fold_net_pnl_ticks') IS DISTINCT FROM 'array'
       OR jsonb_typeof(state -> 'fold_trade_counts') IS DISTINCT FROM 'array'
       OR jsonb_typeof(state -> 'trade_shards') IS DISTINCT FROM 'array'
       OR jsonb_array_length(state -> 'active_session_ids') > 100000
       OR jsonb_array_length(state -> 'fold_net_pnl_ticks') > 10000
       OR jsonb_array_length(state -> 'fold_trade_counts') > 10000
       OR jsonb_array_length(state -> 'trade_shards') > 100000
       OR jsonb_array_length(state -> 'fold_net_pnl_ticks')
            <> jsonb_array_length(state -> 'fold_trade_counts')
       OR systematic_fx.m0b_json_has_forbidden_reference(
            state - 'trade_shards' - 'result_artifact') THEN
        RETURN false;
    END IF;
    IF jsonb_typeof(evaluation_policy) IS DISTINCT FROM 'object'
       OR evaluation_policy #>> '{artifact_schema}'
            IS DISTINCT FROM 'systematic_fx.m0b_worker_evaluation_policy.v1'
       OR (evaluation_policy #>> '{max_signals}' ~ '^[1-9][0-9]*$') IS NOT TRUE
       OR (evaluation_policy #>> '{max_trades}' ~ '^[1-9][0-9]*$') IS NOT TRUE
       OR (evaluation_policy #>> '{search_fold_count}' ~ '^[1-9][0-9]*$') IS NOT TRUE
       OR (evaluation_policy #>> '{stress_extra_cost_ticks}'
            ~ '^(0|[1-9][0-9]*)$') IS NOT TRUE THEN
        RETURN false;
    END IF;

    FOREACH field_name IN ARRAY ARRAY[
        'accepted_tp_count', 'ineligible_signal_count',
        'matching_label_count', 'missing_label_count',
        'next_shard_ordinal', 'next_signal_index', 'overlap_signal_count',
        'raw_event_count', 'raw_tp_count', 'sequential_trade_count']::text[]
    LOOP
        IF (jsonb_typeof(state -> field_name) = 'number'
            AND state ->> field_name ~ '^(0|[1-9][0-9]*)$'
            AND length(state ->> field_name) <= 19) IS NOT TRUE THEN
            RETURN false;
        END IF;
    END LOOP;
    FOREACH field_name IN ARRAY ARRAY[
        'raw_net_pnl_ticks', 'sequential_net_pnl_ticks',
        'sequential_stressed_net_pnl_ticks']::text[]
    LOOP
        IF (jsonb_typeof(state -> field_name) = 'number'
            AND state ->> field_name ~ '^-?(0|[1-9][0-9]*)$'
            AND length(state ->> field_name) <= 20) IS NOT TRUE THEN
            RETURN false;
        END IF;
    END LOOP;
    IF NOT (
        jsonb_typeof(state -> 'next_available_ts_ns') = 'null'
        OR (jsonb_typeof(state -> 'next_available_ts_ns') = 'number'
            AND state #>> '{next_available_ts_ns}' ~ '^-?(0|[1-9][0-9]*)$'
            AND length(state #>> '{next_available_ts_ns}') <= 20)
    ) THEN
        RETURN false;
    END IF;
    IF EXISTS (
        SELECT 1
          FROM jsonb_array_elements(state -> 'active_session_ids') AS item(value)
         WHERE jsonb_typeof(value) IS DISTINCT FROM 'string'
            OR length(value #>> '{}') > 128
            OR systematic_fx.m0b_json_has_forbidden_reference(value)
    ) OR EXISTS (
        SELECT 1
          FROM jsonb_array_elements(state -> 'fold_net_pnl_ticks') AS item(value)
         WHERE (jsonb_typeof(value) = 'number'
                AND value #>> '{}' ~ '^-?(0|[1-9][0-9]*)$'
                AND length(value #>> '{}') <= 20) IS NOT TRUE
    ) OR EXISTS (
        SELECT 1
          FROM jsonb_array_elements(state -> 'fold_trade_counts') AS item(value)
         WHERE (jsonb_typeof(value) = 'number'
                AND value #>> '{}' ~ '^(0|[1-9][0-9]*)$'
                AND length(value #>> '{}') <= 19) IS NOT TRUE
    ) THEN
        RETURN false;
    END IF;

    FOR shard IN SELECT value FROM jsonb_array_elements(state -> 'trade_shards')
    LOOP
        shard_index := shard_index + 1;
        shard_sha256 := shard #>> '{content_sha256}';
        IF jsonb_typeof(shard) IS DISTINCT FROM 'object'
           OR (SELECT array_agg(key ORDER BY key)
                 FROM jsonb_object_keys(shard) AS key)
                IS DISTINCT FROM ARRAY[
                    'byte_size', 'content_sha256', 'first_store_shard',
                    'last_store_shard', 'ordinal', 'relative_uri',
                    'row_count']::text[]
           OR (shard_sha256 ~ '^[0-9a-f]{64}$') IS NOT TRUE
           OR shard #>> '{ordinal}' IS DISTINCT FROM shard_index::text
           OR shard #>> '{relative_uri}' IS DISTINCT FROM format(
                'candidate-trades-%s-%s.json',
                lpad(shard_index::text, 6, '0'), shard_sha256)
           OR systematic_fx.m0b_json_has_forbidden_reference(
                shard - 'relative_uri') THEN
            RETURN false;
        END IF;
        FOREACH field_name IN ARRAY ARRAY[
            'byte_size', 'first_store_shard', 'last_store_shard',
            'ordinal', 'row_count']::text[]
        LOOP
            IF (jsonb_typeof(shard -> field_name) = 'number'
                AND shard ->> field_name ~ '^(0|[1-9][0-9]*)$'
                AND length(shard ->> field_name) <= 10) IS NOT TRUE THEN
                RETURN false;
            END IF;
        END LOOP;
        IF (shard #>> '{byte_size}')::numeric < 1
           OR (shard #>> '{first_store_shard}')::numeric
                <> expected_store_shard
           OR (shard #>> '{last_store_shard}')::numeric
                < expected_store_shard THEN
            RETURN false;
        END IF;
        expected_store_shard := (shard #>> '{last_store_shard}')::numeric + 1;
        shard_row_total := shard_row_total + (shard #>> '{row_count}')::numeric;
    END LOOP;
    IF (state #>> '{next_shard_ordinal}')::numeric <> expected_store_shard THEN
        RETURN false;
    END IF;

    sequential_count := (state #>> '{sequential_trade_count}')::numeric;
    raw_count := (state #>> '{raw_event_count}')::numeric;
    matching_count := (state #>> '{matching_label_count}')::numeric;
    signal_index := (state #>> '{next_signal_index}')::numeric;
    stress_cost := (evaluation_policy #>> '{stress_extra_cost_ticks}')::numeric;
    SELECT COALESCE(sum(value::numeric), 0)
      INTO fold_trade_total
      FROM jsonb_array_elements_text(state -> 'fold_trade_counts') AS item(value);
    SELECT COALESCE(sum(value::numeric), 0)
      INTO fold_pnl_total
      FROM jsonb_array_elements_text(state -> 'fold_net_pnl_ticks') AS item(value);
    IF jsonb_array_length(state -> 'fold_trade_counts')
            <> (evaluation_policy #>> '{search_fold_count}')::integer
       OR sequential_count > (evaluation_policy #>> '{max_trades}')::numeric
       OR signal_index > (evaluation_policy #>> '{max_signals}')::numeric
       OR signal_index <> matching_count
            + (state #>> '{missing_label_count}')::numeric
       OR matching_count <> raw_count
            + (state #>> '{ineligible_signal_count}')::numeric
       OR (state #>> '{raw_tp_count}')::numeric > raw_count
       OR (state #>> '{accepted_tp_count}')::numeric
            > (state #>> '{raw_tp_count}')::numeric
       OR raw_count <> sequential_count
            + (state #>> '{overlap_signal_count}')::numeric
       OR (state #>> '{accepted_tp_count}')::numeric > sequential_count
       OR jsonb_array_length(state -> 'active_session_ids') > sequential_count
       OR (SELECT count(*)
             FROM jsonb_array_elements_text(state -> 'active_session_ids'))
          <> (SELECT count(DISTINCT value)
                FROM jsonb_array_elements_text(
                    state -> 'active_session_ids') AS item(value))
       OR shard_row_total <> sequential_count
       OR fold_trade_total <> sequential_count
       OR fold_pnl_total <> (state #>> '{sequential_net_pnl_ticks}')::numeric
       OR (state #>> '{sequential_stressed_net_pnl_ticks}')::numeric
            <> (state #>> '{sequential_net_pnl_ticks}')::numeric
               - stress_cost * sequential_count THEN
        RETURN false;
    END IF;

    result_identity := state -> 'result_artifact';
    IF (state ->> 'complete')::boolean THEN
        IF jsonb_typeof(result_identity) IS DISTINCT FROM 'object'
           OR (SELECT array_agg(key ORDER BY key)
                FROM jsonb_object_keys(result_identity) AS key)
                IS DISTINCT FROM ARRAY[
                    'byte_size', 'classification', 'content_sha256', 'metrics',
                    'relative_uri']::text[]
           OR (jsonb_typeof(result_identity -> 'byte_size') = 'number'
               AND result_identity #>> '{byte_size}' ~ '^[1-9][0-9]*$'
               AND length(result_identity #>> '{byte_size}') <= 8
               AND (result_identity #>> '{byte_size}')::numeric <= 67108864)
                IS NOT TRUE
           OR (result_identity #>> '{classification}'
                IN ('SCREENED_OUT', 'REGISTERED')) IS NOT TRUE
           OR (result_identity #>> '{content_sha256}' ~ '^[0-9a-f]{64}$')
                IS NOT TRUE
           OR result_identity #>> '{relative_uri}' IS DISTINCT FROM format(
                'candidate-result-%s.json',
                result_identity #>> '{content_sha256}')
           OR systematic_fx.m0b_numeric_metrics_valid(
                result_identity -> 'metrics') IS NOT TRUE
           OR result_identity -> 'metrics' IS DISTINCT FROM jsonb_build_object(
                'active_days', jsonb_array_length(state -> 'active_session_ids'),
                'flat_trades', (state #>> '{sequential_trade_count}')::bigint,
                'net_pnl_ticks', (state #>> '{sequential_net_pnl_ticks}')::bigint,
                'positive_search_folds', (
                    SELECT count(*)::integer
                      FROM jsonb_array_elements_text(
                          state -> 'fold_net_pnl_ticks') AS fold(value)
                     WHERE value::numeric > 0),
                'raw_events', (state #>> '{raw_event_count}')::bigint,
                'sequential_trades',
                    (state #>> '{sequential_trade_count}')::bigint,
                'stressed_net_pnl_ticks',
                    (state #>> '{sequential_stressed_net_pnl_ticks}')::bigint,
                'tp_probability_ppm', CASE
                    WHEN (state #>> '{sequential_trade_count}')::numeric = 0 THEN 0
                    ELSE floor(
                        ((state #>> '{accepted_tp_count}')::numeric * 1000000
                         + floor(
                             (state #>> '{sequential_trade_count}')::numeric / 2))
                        / (state #>> '{sequential_trade_count}')::numeric)
                END::bigint)
           OR systematic_fx.m0b_json_has_forbidden_reference(
                result_identity - 'relative_uri') THEN
            RETURN false;
        END IF;
    ELSIF jsonb_typeof(result_identity) IS DISTINCT FROM 'null' THEN
        RETURN false;
    END IF;
    RETURN true;
EXCEPTION WHEN OTHERS THEN
    RETURN false;
END;
$$;

-- Replace the 0029 generic checkpoint trigger with the exact executable M0b
-- cursor contract.  Identity fields remain fully content-addressed and the
-- only admitted URI values are the leaf names validated above.
CREATE OR REPLACE FUNCTION systematic_fx.validate_m0b_checkpoint_insert()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE expected_spec bigint; actual_spec bigint; expected_predecessor text;
        expected_sequence integer; candidate_status text; attempt_status text;
        epoch_status text; campaign_status text; holdout_revealed_at timestamptz;
        closed_at timestamptz; expected_work_sha256 text;
        expected_evaluation_policy jsonb;
        predecessor_complete boolean;
BEGIN
    SELECT candidate.research_run_spec_id, candidate.status, epoch.status,
           campaign.status, campaign.holdout_revealed_at, campaign.closed_at,
           work_artifact.sha256,
           work_artifact.metadata -> 'evaluation_policy'
      INTO STRICT expected_spec, candidate_status, epoch_status, campaign_status,
           holdout_revealed_at, closed_at, expected_work_sha256,
           expected_evaluation_policy
      FROM systematic_fx.m0b_candidates AS candidate
      JOIN systematic_fx.m0b_epochs AS epoch USING (m0b_epoch_id)
      JOIN systematic_fx.campaigns AS campaign USING (campaign_id)
      JOIN systematic_fx.artifacts AS work_artifact
        ON work_artifact.artifact_id = candidate.work_artifact_id
     WHERE candidate.m0b_candidate_id = NEW.m0b_candidate_id
     FOR SHARE OF candidate, epoch, campaign;
    SELECT research_run_spec_id, status INTO STRICT actual_spec, attempt_status
      FROM systematic_fx.research_run_attempts
     WHERE research_run_attempt_id = NEW.research_run_attempt_id FOR SHARE;
    IF actual_spec <> expected_spec THEN
        RAISE EXCEPTION 'M0b checkpoint attempt/candidate mismatch';
    END IF;
    IF candidate_status <> 'RUNNING' OR attempt_status <> 'RUNNING'
       OR epoch_status <> 'RUNNING' OR campaign_status <> 'RUNNING'
       OR holdout_revealed_at IS NOT NULL OR closed_at IS NOT NULL THEN
        RAISE EXCEPTION 'M0b checkpoints require an active unrevealed search attempt';
    END IF;
    SELECT COALESCE(max(checkpoint_sequence), 0) + 1,
           (array_agg(checkpoint_sha256 ORDER BY checkpoint_sequence DESC))[1],
           (array_agg((cursor #>> '{state,complete}')::boolean
                      ORDER BY checkpoint_sequence DESC))[1]
      INTO expected_sequence, expected_predecessor, predecessor_complete
      FROM systematic_fx.m0b_checkpoints
     WHERE research_run_attempt_id = NEW.research_run_attempt_id;
    IF NEW.checkpoint_sequence <> expected_sequence
       OR NEW.predecessor_sha256 IS DISTINCT FROM expected_predecessor THEN
        RAISE EXCEPTION 'M0b checkpoint chain is not contiguous';
    END IF;
    IF predecessor_complete IS TRUE THEN
        RAISE EXCEPTION 'M0b complete checkpoint is terminal for its attempt';
    END IF;
    IF systematic_fx.m0b_json_has_forbidden_reference(NEW.cursor - 'state')
       OR systematic_fx.m0b_worker_checkpoint_state_valid(
            NEW.cursor -> 'state', expected_evaluation_policy) IS NOT TRUE
       OR NEW.cursor #>> '{state,work_spec_sha256}'
            IS DISTINCT FROM expected_work_sha256 THEN
        RAISE EXCEPTION 'M0b checkpoint contains a forbidden external reference or state';
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
            IS DISTINCT FROM COALESCE(
                to_jsonb(NEW.predecessor_sha256), 'null'::jsonb)
       OR jsonb_array_length(NEW.cursor #> '{state,trade_shards}')
            <> NEW.checkpoint_sequence THEN
        RAISE EXCEPTION 'M0b checkpoint canonical identity mismatch';
    END IF;
    RETURN NEW;
END;
$$;

-- Keep every 0029 epoch invariant, replacing only the placeholder admission
-- marker with the exact numeric v1 contract.
CREATE OR REPLACE FUNCTION systematic_fx.validate_m0b_epoch_insert()
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
       OR NOT systematic_fx.m0b_numeric_admission_rules_valid(
            NEW.canonical_epoch -> 'admission_rules')
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
        SELECT 1 FROM systematic_fx.research_run_specs
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

-- 0029 candidates remain readable after upgrade.  Every candidate registered
-- through the 0030 API binds one immutable work artifact at INSERT time; an
-- unbound legacy row is deliberately unclaimable by the worker capability.
ALTER TABLE systematic_fx.m0b_candidates
    ADD COLUMN work_artifact_id bigint;
ALTER TABLE systematic_fx.m0b_candidates
    ADD CONSTRAINT m0b_candidates_work_artifact_fk
        FOREIGN KEY (work_artifact_id)
        REFERENCES systematic_fx.artifacts(artifact_id),
    ADD CONSTRAINT m0b_candidates_work_artifact_unique
        UNIQUE (work_artifact_id);

-- The human-readable candidate document keeps the small grid values as exact
-- decimals/minutes, while executable labels use exact rational distances
-- and seconds.  This conversion is checked in the database so a work artifact
-- cannot silently evaluate another bracket than the registered candidate.
CREATE FUNCTION systematic_fx.m0b_candidate_work_barrier_matches(
    candidate jsonb,
    barrier jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
SET search_path = pg_catalog AS $$
DECLARE
    tp_num bigint; tp_den bigint; sl_num bigint; sl_den bigint;
    hold_seconds bigint; hold_minutes bigint;
BEGIN
    IF jsonb_typeof(candidate) IS DISTINCT FROM 'object'
       OR jsonb_typeof(barrier) IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key)
             FROM jsonb_object_keys(barrier) AS key)
            IS DISTINCT FROM ARRAY[
                'artifact_schema', 'barrier_id', 'k_sl_den', 'k_sl_num',
                'k_tp_den', 'k_tp_num', 'max_hold_seconds']::text[]
       OR barrier #>> '{artifact_schema}'
            IS DISTINCT FROM 'systematic_fx.m0b_volatility_barrier.v1'
       OR jsonb_typeof(candidate #> '{barrier,k_tp}') IS DISTINCT FROM 'string'
       OR jsonb_typeof(candidate #> '{barrier,k_sl}') IS DISTINCT FROM 'string'
       OR jsonb_typeof(candidate #> '{barrier,max_hold_minutes}')
            IS DISTINCT FROM 'number'
       OR (candidate #>> '{barrier,k_tp}'
            ~ '^(0|[1-9][0-9]*)\.[0-9]{2}$') IS NOT TRUE
       OR (candidate #>> '{barrier,k_sl}'
            ~ '^(0|[1-9][0-9]*)\.[0-9]{2}$') IS NOT TRUE
       OR (candidate #>> '{barrier,max_hold_minutes}'
            ~ '^[1-9][0-9]*$') IS NOT TRUE
       OR length(candidate #>> '{barrier,k_tp}') > 20
       OR length(candidate #>> '{barrier,k_sl}') > 20
       OR length(candidate #>> '{barrier,max_hold_minutes}') > 8 THEN
        RETURN false;
    END IF;
    IF EXISTS (
        SELECT 1 FROM unnest(ARRAY[
            'k_tp_num', 'k_tp_den', 'k_sl_num', 'k_sl_den',
            'max_hold_seconds']::text[]) AS field(name)
         WHERE (jsonb_typeof(barrier -> name) = 'number'
                AND barrier ->> name ~ '^[1-9][0-9]*$'
                AND length(barrier ->> name) <= 10) IS NOT TRUE
    ) THEN
        RETURN false;
    END IF;
    tp_num := (barrier #>> '{k_tp_num}')::bigint;
    tp_den := (barrier #>> '{k_tp_den}')::bigint;
    sl_num := (barrier #>> '{k_sl_num}')::bigint;
    sl_den := (barrier #>> '{k_sl_den}')::bigint;
    hold_seconds := (barrier #>> '{max_hold_seconds}')::bigint;
    hold_minutes := (candidate #>> '{barrier,max_hold_minutes}')::bigint;
    IF tp_num > 1000000 OR tp_den > 1000000
       OR sl_num > 1000000 OR sl_den > 1000000
       OR hold_seconds > 31536000 OR hold_minutes > 525600
       OR tp_num::numeric / tp_den::numeric
            <> (candidate #>> '{barrier,k_tp}')::numeric
       OR sl_num::numeric / sl_den::numeric
            <> (candidate #>> '{barrier,k_sl}')::numeric
       OR hold_seconds <> hold_minutes * 60
       OR barrier #>> '{barrier_id}' IS DISTINCT FROM format(
            'tp%sof%s_sl%sof%s_h%s', tp_num, tp_den, sl_num, sl_den,
            hold_seconds) THEN
        RETURN false;
    END IF;
    RETURN true;
EXCEPTION WHEN OTHERS THEN
    RETURN false;
END;
$$;

CREATE FUNCTION systematic_fx.validate_m0b_candidate_work_insert()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE artifact_record record; epoch_record record; run_record record;
        expected_rules_sha256 text;
BEGIN
    SELECT canonical_epoch, epoch_sha256, code_snapshot_sha256,
           feature_sha256, label_sha256, cost_sha256, execution_sha256,
           split_sha256
      INTO STRICT epoch_record
      FROM systematic_fx.m0b_epochs
     WHERE m0b_epoch_id = NEW.m0b_epoch_id FOR SHARE;
    SELECT canonical_spec, deterministic_seed, direction
      INTO STRICT run_record
      FROM systematic_fx.research_run_specs
     WHERE research_run_spec_id = NEW.research_run_spec_id FOR SHARE;
    -- ALTER does not re-fire INSERT triggers for 0029 rows.  Every candidate
    -- newly registered after 0030 must be bound; otherwise a caller could burn
    -- finite epoch budget with permanently unclaimable work.
    IF NEW.work_artifact_id IS NULL THEN
        RAISE EXCEPTION 'M0b candidate requires one immutable CandidateWork artifact';
    END IF;
    SELECT artifact_id, artifact_key, artifact_type, uri, sha256, byte_size,
           media_type, producer_job_id, metadata
      INTO STRICT artifact_record
      FROM systematic_fx.artifacts
     WHERE artifact_id = NEW.work_artifact_id FOR SHARE;
    expected_rules_sha256 := systematic_fx.canonical_jsonb_sha256(
        epoch_record.canonical_epoch -> 'admission_rules');
    IF artifact_record.artifact_type <> 'M0B_CANDIDATE_WORK'
       OR artifact_record.media_type IS DISTINCT FROM 'application/json'
       OR artifact_record.producer_job_id IS NOT NULL
       OR artifact_record.byte_size NOT BETWEEN 1 AND 1048576
       OR artifact_record.sha256 !~ '^[0-9a-f]{64}$'
       OR artifact_record.artifact_key IS DISTINCT FROM format(
            'm0b-candidate-work:%s:%s:%s', epoch_record.epoch_sha256,
            NEW.candidate_sha256, artifact_record.sha256)
       OR artifact_record.uri IS DISTINCT FROM format(
            'm0b-work://search/%s/%s/sha256=%s.json', epoch_record.epoch_sha256,
            NEW.candidate_sha256, artifact_record.sha256)
       OR jsonb_typeof(artifact_record.metadata) IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key)
             FROM jsonb_object_keys(artifact_record.metadata) AS key)
            IS DISTINCT FROM ARRAY[
                'admission_rules_sha256', 'barrier', 'barrier_sha256',
                'candidate_kind', 'candidate_sha256', 'code_snapshot_sha256',
                'cost_sha256', 'data_role', 'deterministic_seed', 'direction',
                'epoch_sha256', 'evaluation_policy',
                'evaluation_policy_sha256', 'execution_sha256',
                'first_passage_store_sha256', 'identity_schema',
                'signal_artifact_sha256', 'source_build_sha256',
                'source_feature_sha256', 'source_label_sha256', 'split_sha256',
                'work_spec_sha256']::text[]
       OR artifact_record.metadata #>> '{identity_schema}'
            IS DISTINCT FROM 'systematic_fx.m0b.candidate_work.v2'
       OR artifact_record.metadata #>> '{data_role}' IS DISTINCT FROM 'SEARCH'
       OR artifact_record.metadata #>> '{epoch_sha256}'
            IS DISTINCT FROM epoch_record.epoch_sha256
       OR artifact_record.metadata #>> '{candidate_sha256}'
            IS DISTINCT FROM NEW.candidate_sha256
       OR artifact_record.metadata #>> '{candidate_kind}'
            IS DISTINCT FROM NEW.candidate_kind
       OR artifact_record.metadata #>> '{work_spec_sha256}'
            IS DISTINCT FROM artifact_record.sha256
       OR run_record.canonical_spec #>> '{parameters,m0b_work_spec_sha256}'
            IS DISTINCT FROM artifact_record.sha256
       OR artifact_record.metadata #>> '{code_snapshot_sha256}'
            IS DISTINCT FROM epoch_record.code_snapshot_sha256
       OR artifact_record.metadata #>> '{code_snapshot_sha256}'
            IS DISTINCT FROM run_record.canonical_spec #>> '{code_snapshot_sha256}'
       OR artifact_record.metadata #>> '{admission_rules_sha256}'
            IS DISTINCT FROM expected_rules_sha256
       OR artifact_record.metadata #>> '{deterministic_seed}'
            IS DISTINCT FROM run_record.deterministic_seed::text
       OR artifact_record.metadata #>> '{deterministic_seed}'
            IS DISTINCT FROM NEW.canonical_candidate #>> '{random_seed}'
       OR artifact_record.metadata #>> '{direction}' IS DISTINCT FROM run_record.direction
       OR artifact_record.metadata #>> '{direction}'
            IS DISTINCT FROM NEW.canonical_candidate #>> '{direction}'
       OR artifact_record.metadata #>> '{cost_sha256}'
            IS DISTINCT FROM epoch_record.cost_sha256
       OR artifact_record.metadata #>> '{cost_sha256}'
            IS DISTINCT FROM NEW.canonical_candidate #>> '{cost,sha256}'
       OR artifact_record.metadata #>> '{execution_sha256}'
            IS DISTINCT FROM epoch_record.execution_sha256
       OR artifact_record.metadata #>> '{split_sha256}'
            IS DISTINCT FROM epoch_record.split_sha256
       OR systematic_fx.m0b_candidate_work_barrier_matches(
            NEW.canonical_candidate, artifact_record.metadata -> 'barrier') IS NOT TRUE
       OR artifact_record.metadata #>> '{barrier_sha256}'
            IS DISTINCT FROM systematic_fx.canonical_jsonb_sha256(
                artifact_record.metadata -> 'barrier')
       OR artifact_record.metadata #>> '{barrier_sha256}'
            IS DISTINCT FROM run_record.canonical_spec #>>
                '{parameters,m0b_barrier_sha256}'
       OR run_record.canonical_spec -> 'barrier_policy'
            IS DISTINCT FROM artifact_record.metadata -> 'barrier'
       OR jsonb_typeof(artifact_record.metadata -> 'evaluation_policy')
            IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key)
             FROM jsonb_object_keys(
                 artifact_record.metadata -> 'evaluation_policy') AS key)
            IS DISTINCT FROM ARRAY[
                'artifact_schema', 'checkpoint_shard_interval', 'cooldown_ns',
                'max_signals', 'max_trades', 'search_fold_count',
                'stress_extra_cost_ticks']::text[]
       OR artifact_record.metadata #>> '{evaluation_policy,artifact_schema}'
            IS DISTINCT FROM 'systematic_fx.m0b_worker_evaluation_policy.v1'
       OR artifact_record.metadata #>> '{evaluation_policy_sha256}'
            IS DISTINCT FROM systematic_fx.canonical_jsonb_sha256(
                artifact_record.metadata -> 'evaluation_policy')
       OR artifact_record.metadata #>> '{evaluation_policy_sha256}'
            IS DISTINCT FROM run_record.canonical_spec #>>
                '{parameters,m0b_evaluation_policy_sha256}'
       OR EXISTS (
            SELECT 1 FROM unnest(ARRAY[
                'checkpoint_shard_interval', 'max_signals', 'max_trades',
                'search_fold_count']::text[]) AS field(name)
             WHERE (jsonb_typeof(
                        artifact_record.metadata #> ARRAY['evaluation_policy', name])
                        = 'number'
                    AND artifact_record.metadata #>> ARRAY['evaluation_policy', name]
                        ~ '^[1-9][0-9]*$'
                    AND length(artifact_record.metadata #>>
                        ARRAY['evaluation_policy', name]) <= 10) IS NOT TRUE)
       OR EXISTS (
            SELECT 1 FROM unnest(ARRAY[
                'cooldown_ns', 'stress_extra_cost_ticks']::text[]) AS field(name)
             WHERE (jsonb_typeof(
                        artifact_record.metadata #> ARRAY['evaluation_policy', name])
                        = 'number'
                    AND artifact_record.metadata #>> ARRAY['evaluation_policy', name]
                        ~ '^(0|[1-9][0-9]*)$'
                    AND length(artifact_record.metadata #>>
                        ARRAY['evaluation_policy', name]) <= 19) IS NOT TRUE)
       OR (artifact_record.metadata #>>
            '{evaluation_policy,checkpoint_shard_interval}')::numeric > 100000
       OR (artifact_record.metadata #>>
            '{evaluation_policy,max_signals}')::numeric > 1000000
       OR (artifact_record.metadata #>>
            '{evaluation_policy,max_trades}')::numeric > 1000000
       OR (artifact_record.metadata #>>
            '{evaluation_policy,search_fold_count}')::numeric > 10000
       OR (artifact_record.metadata #>>
            '{evaluation_policy,cooldown_ns}')::numeric > 31536000000000000
       OR (artifact_record.metadata #>>
            '{evaluation_policy,stress_extra_cost_ticks}')::numeric > 1000000
       OR (artifact_record.metadata #>> '{evaluation_policy,max_trades}')::numeric
            > (artifact_record.metadata #>> '{evaluation_policy,max_signals}')::numeric
       OR artifact_record.metadata #>> '{source_feature_sha256}'
            IS DISTINCT FROM epoch_record.feature_sha256
       OR artifact_record.metadata #>> '{source_label_sha256}'
            IS DISTINCT FROM epoch_record.label_sha256
       OR artifact_record.metadata #>> '{first_passage_store_sha256}'
            !~ '^[0-9a-f]{64}$'
       OR artifact_record.metadata #>> '{signal_artifact_sha256}'
            !~ '^[0-9a-f]{64}$'
       OR artifact_record.metadata #>> '{source_build_sha256}'
            IS DISTINCT FROM epoch_record.canonical_epoch #>> '{dataset,sha256}'
       OR systematic_fx.m0b_json_has_forbidden_reference(artifact_record.metadata) THEN
        RAISE EXCEPTION 'M0b CandidateWork artifact identity differs from candidate provenance';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER m0b_candidates_validate_work_insert
BEFORE INSERT ON systematic_fx.m0b_candidates
FOR EACH ROW EXECUTE FUNCTION systematic_fx.validate_m0b_candidate_work_insert();

CREATE OR REPLACE FUNCTION systematic_fx.protect_m0b_candidate()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M0b candidates are append-preserved';
    END IF;
    IF ROW(NEW.m0b_epoch_id, NEW.parent_candidate_id, NEW.research_run_spec_id,
           NEW.work_artifact_id, NEW.candidate_kind, NEW.ordinal,
           NEW.candidate_sha256, NEW.canonical_candidate, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.m0b_epoch_id, OLD.parent_candidate_id, OLD.research_run_spec_id,
           OLD.work_artifact_id, OLD.candidate_kind, OLD.ordinal,
           OLD.candidate_sha256, OLD.canonical_candidate, OLD.created_at) THEN
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

-- Extend only the exact 0029 governed RunSpec contract with the immutable
-- CandidateWork hash.  Its canonical fingerprint therefore commits to the
-- bytes before candidate registration and worker claim.
CREATE OR REPLACE FUNCTION systematic_fx.protect_m0b_run_spec_lineage()
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
                'data_role', 'm0b_barrier_sha256', 'm0b_candidate_sha256',
                'm0b_contract_reference_sha256', 'm0b_dataset_sha256',
                'm0b_epoch_sha256', 'm0b_evaluation_policy_sha256',
                'm0b_work_spec_sha256', 'split_role']::text[]
             ELSE ARRAY[
                'data_role', 'm0b_barrier_sha256', 'm0b_candidate_sha256',
                'm0b_contract_reference_sha256', 'm0b_dataset_sha256',
                'm0b_epoch_sha256', 'm0b_evaluation_policy_sha256',
                'm0b_work_spec_sha256',
                'parent_run_fingerprint', 'split_role']::text[] END)
       OR NEW.canonical_spec #>> '{parameters,m0b_dataset_sha256}'
            IS DISTINCT FROM epoch_record.dataset_sha256
       OR NEW.canonical_spec #>> '{parameters,m0b_contract_reference_sha256}'
            IS DISTINCT FROM epoch_record.contract_reference_sha256
       OR NEW.canonical_spec #>> '{parameters,m0b_candidate_sha256}' IS NULL
       OR NEW.canonical_spec #>> '{parameters,m0b_barrier_sha256}'
            IS DISTINCT FROM systematic_fx.canonical_jsonb_sha256(
                NEW.canonical_spec -> 'barrier_policy')
       OR NEW.canonical_spec #>> '{parameters,m0b_evaluation_policy_sha256}'
            !~ '^[0-9a-f]{64}$'
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
       OR jsonb_typeof(NEW.canonical_spec -> 'barrier_policy')
            IS DISTINCT FROM 'object'
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

CREATE TABLE systematic_fx.m0b_admission_decisions (
    m0b_admission_decision_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    m0b_candidate_id bigint NOT NULL UNIQUE,
    research_run_attempt_id bigint NOT NULL UNIQUE,
    result_artifact_id bigint NOT NULL UNIQUE,
    admission_rules_sha256 text NOT NULL,
    metrics jsonb NOT NULL,
    metrics_sha256 text NOT NULL,
    classification text NOT NULL,
    evaluated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT m0b_admission_decisions_candidate_fk FOREIGN KEY (m0b_candidate_id)
        REFERENCES systematic_fx.m0b_candidates(m0b_candidate_id),
    CONSTRAINT m0b_admission_decisions_attempt_fk FOREIGN KEY (research_run_attempt_id)
        REFERENCES systematic_fx.research_run_attempts(research_run_attempt_id),
    CONSTRAINT m0b_admission_decisions_result_artifact_fk FOREIGN KEY (result_artifact_id)
        REFERENCES systematic_fx.artifacts(artifact_id),
    CONSTRAINT m0b_admission_decisions_hashes_valid CHECK (
        admission_rules_sha256 ~ '^[0-9a-f]{64}$'
        AND metrics_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT m0b_admission_decisions_metrics_object CHECK (jsonb_typeof(metrics) = 'object'),
    CONSTRAINT m0b_admission_decisions_classification_valid CHECK (
        classification IN ('SCREENED_OUT', 'REGISTERED'))
);

CREATE TABLE systematic_fx.m0b_worker_leases (
    research_run_attempt_id bigint PRIMARY KEY,
    m0b_candidate_id bigint NOT NULL,
    login_role text NOT NULL,
    worker_id text NOT NULL,
    lease_token_sha256 text NOT NULL UNIQUE,
    status text NOT NULL DEFAULT 'ACTIVE',
    failure_retryable boolean,
    failure_resulting_status text,
    leased_until timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    released_at timestamptz,
    CONSTRAINT m0b_worker_leases_attempt_fk FOREIGN KEY (research_run_attempt_id)
        REFERENCES systematic_fx.research_run_attempts(research_run_attempt_id),
    CONSTRAINT m0b_worker_leases_candidate_fk FOREIGN KEY (m0b_candidate_id)
        REFERENCES systematic_fx.m0b_candidates(m0b_candidate_id),
    CONSTRAINT m0b_worker_leases_worker_nonempty CHECK (
        btrim(worker_id) <> '' AND length(worker_id) <= 128
        AND btrim(login_role) <> '' AND length(login_role) <= 128),
    CONSTRAINT m0b_worker_leases_token_valid CHECK (
        lease_token_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT m0b_worker_leases_status_valid CHECK (status IN ('ACTIVE', 'RELEASED')),
    CONSTRAINT m0b_worker_leases_failure_shape CHECK (
        (failure_retryable IS NULL AND failure_resulting_status IS NULL)
        OR (status = 'RELEASED' AND failure_retryable IS NOT NULL
            AND failure_resulting_status IN ('RUNNING', 'FAILED'))),
    CONSTRAINT m0b_worker_leases_lifecycle_shape CHECK (
        (status = 'ACTIVE' AND released_at IS NULL)
        OR (status = 'RELEASED' AND released_at IS NOT NULL
            AND released_at >= created_at))
);
CREATE UNIQUE INDEX m0b_worker_leases_one_active_candidate
    ON systematic_fx.m0b_worker_leases (m0b_candidate_id)
    WHERE status = 'ACTIVE';

CREATE FUNCTION systematic_fx.validate_m0b_admission_decision_insert()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE candidate_record record; artifact_record record; expected_classification text;
BEGIN
    SELECT candidate.candidate_kind, candidate.status AS candidate_status,
           candidate.research_run_spec_id, attempt.research_run_spec_id AS attempt_spec_id,
           attempt.status AS attempt_status,
           epoch.canonical_epoch -> 'admission_rules' AS rules,
           systematic_fx.canonical_jsonb_sha256(
               epoch.canonical_epoch -> 'admission_rules') AS rules_sha256,
           epoch.status AS epoch_status, campaign.status AS campaign_status,
           campaign.holdout_revealed_at, campaign.closed_at
      INTO STRICT candidate_record
      FROM systematic_fx.m0b_candidates AS candidate
      JOIN systematic_fx.m0b_epochs AS epoch USING (m0b_epoch_id)
      JOIN systematic_fx.campaigns AS campaign USING (campaign_id)
      JOIN systematic_fx.research_run_attempts AS attempt
        ON attempt.research_run_attempt_id = NEW.research_run_attempt_id
     WHERE candidate.m0b_candidate_id = NEW.m0b_candidate_id
     FOR SHARE OF candidate, epoch, campaign, attempt;
    expected_classification := CASE
        WHEN candidate_record.candidate_kind = 'REAL'
         AND systematic_fx.m0b_numeric_metrics_admitted(
             candidate_record.rules, NEW.metrics)
        THEN 'REGISTERED' ELSE 'SCREENED_OUT' END;
    SELECT artifact_type, metadata INTO STRICT artifact_record
      FROM systematic_fx.artifacts
     WHERE artifact_id = NEW.result_artifact_id;
    IF candidate_record.candidate_status <> 'RUNNING'
       OR candidate_record.attempt_status <> 'RUNNING'
       OR candidate_record.research_run_spec_id <> candidate_record.attempt_spec_id
       OR candidate_record.epoch_status <> 'RUNNING'
       OR candidate_record.campaign_status <> 'RUNNING'
       OR candidate_record.holdout_revealed_at IS NOT NULL
       OR candidate_record.closed_at IS NOT NULL
       OR NOT systematic_fx.m0b_numeric_metrics_valid(NEW.metrics)
       OR NEW.metrics_sha256 IS DISTINCT FROM
            systematic_fx.canonical_jsonb_sha256(NEW.metrics)
       OR NEW.admission_rules_sha256 IS DISTINCT FROM candidate_record.rules_sha256
       OR NEW.classification IS DISTINCT FROM expected_classification
       OR artifact_record.artifact_type <> 'M0B_RESULT'
       OR artifact_record.metadata #>> '{m0b_candidate_id}'
            IS DISTINCT FROM NEW.m0b_candidate_id::text
       OR artifact_record.metadata #>> '{research_run_attempt_id}'
            IS DISTINCT FROM NEW.research_run_attempt_id::text THEN
        RAISE EXCEPTION 'M0b admission decision differs from immutable numeric rules';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER m0b_admission_decisions_validate_insert
BEFORE INSERT ON systematic_fx.m0b_admission_decisions
FOR EACH ROW EXECUTE FUNCTION systematic_fx.validate_m0b_admission_decision_insert();
CREATE TRIGGER m0b_admission_decisions_immutable
BEFORE UPDATE OR DELETE ON systematic_fx.m0b_admission_decisions
FOR EACH ROW EXECUTE FUNCTION systematic_fx.reject_m0b_identity_mutation();

CREATE OR REPLACE FUNCTION systematic_fx.validate_m0b_artifact_link_insert()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE expected_spec bigint; actual_spec bigint; attempt_status text;
        candidate_status text; epoch_status text; campaign_status text;
        holdout_revealed_at timestamptz; closed_at timestamptz;
        artifact_record record; attempt_result_artifact_id bigint;
        m0b_epoch_id bigint; terminal_metrics_sha256 text;
        epoch_sha256 text; candidate_sha256 text; admission_rules_sha256 text;
BEGIN
    SELECT candidate.research_run_spec_id, candidate.status, epoch.status,
           campaign.status, campaign.holdout_revealed_at, campaign.closed_at,
           epoch.m0b_epoch_id, epoch.epoch_sha256, candidate.candidate_sha256,
           systematic_fx.canonical_jsonb_sha256(epoch.canonical_epoch -> 'admission_rules')
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
    IF actual_spec <> expected_spec THEN
        RAISE EXCEPTION 'M0b artifact attempt/candidate mismatch';
    END IF;
    IF epoch_status <> 'RUNNING' OR campaign_status <> 'RUNNING'
       OR holdout_revealed_at IS NOT NULL OR closed_at IS NOT NULL
       OR candidate_status <> 'RUNNING' THEN
        RAISE EXCEPTION 'M0b artifact links require an active unrevealed search epoch';
    END IF;
    SELECT artifact_type, sha256, byte_size, metadata INTO STRICT artifact_record
      FROM systematic_fx.artifacts WHERE artifact_id = NEW.artifact_id;
    IF artifact_record.sha256 IS DISTINCT FROM NEW.artifact_sha256
       OR artifact_record.byte_size IS DISTINCT FROM NEW.artifact_byte_size THEN
        RAISE EXCEPTION 'M0b linked artifact byte identity mismatch';
    END IF;
    IF NEW.artifact_role = 'RESULT' THEN
        SELECT metrics_sha256 INTO STRICT terminal_metrics_sha256
          FROM systematic_fx.m0b_admission_decisions
         WHERE m0b_candidate_id = NEW.m0b_candidate_id
           AND research_run_attempt_id = NEW.research_run_attempt_id
           AND result_artifact_id = NEW.artifact_id;
        IF attempt_status <> 'SUCCEEDED'
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
                'admission_rules_sha256', admission_rules_sha256) THEN
            RAISE EXCEPTION 'M0b RESULT link must bind exact numeric search evidence';
        END IF;
    ELSIF NEW.artifact_role = 'FAILURE' AND attempt_status <> 'FAILED' THEN
        RAISE EXCEPTION 'M0b FAILURE link requires a failed attempt';
    ELSIF NEW.artifact_role = 'DETAIL' AND candidate_status <> 'RUNNING' THEN
        RAISE EXCEPTION 'M0b detail artifacts may be linked only while candidate is running';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION systematic_fx.protect_m0b_attempt_lifecycle()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    candidate_record record;
    result_artifact_record record;
    decision_record record;
    attempt_count integer;
    governed_epoch_id bigint;
BEGIN
    SELECT candidate.m0b_candidate_id, candidate.status AS candidate_status,
           epoch.status AS epoch_status, epoch.max_attempts_per_candidate,
           epoch.epoch_sha256, candidate.candidate_sha256,
           systematic_fx.canonical_jsonb_sha256(
               epoch.canonical_epoch -> 'admission_rules') AS admission_rules_sha256,
           campaign.status AS campaign_status, campaign.holdout_revealed_at,
           campaign.closed_at
      INTO candidate_record
      FROM systematic_fx.m0b_candidates AS candidate
      JOIN systematic_fx.m0b_epochs AS epoch USING (m0b_epoch_id)
      JOIN systematic_fx.campaigns AS campaign USING (campaign_id)
     WHERE candidate.research_run_spec_id = NEW.research_run_spec_id FOR SHARE;
    IF NOT FOUND THEN
        SELECT epoch.m0b_epoch_id INTO governed_epoch_id
          FROM systematic_fx.research_run_specs AS run_spec
          JOIN systematic_fx.m0b_epochs AS epoch ON epoch.campaign_id = run_spec.campaign_id
         WHERE run_spec.research_run_spec_id = NEW.research_run_spec_id;
        IF governed_epoch_id IS NOT NULL THEN
            RAISE EXCEPTION 'M0b campaign RunSpecs require a budgeted candidate before attempts';
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
               AND NEW.trade_ledger_artifact_id IS DISTINCT FROM OLD.trade_ledger_artifact_id) THEN
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
             WHERE artifact_id = NEW.result_artifact_id;
            SELECT metrics_sha256, classification INTO STRICT decision_record
              FROM systematic_fx.m0b_admission_decisions
             WHERE m0b_candidate_id = candidate_record.m0b_candidate_id
               AND research_run_attempt_id = NEW.research_run_attempt_id
               AND result_artifact_id = NEW.result_artifact_id;
            IF result_artifact_record.artifact_type <> 'M0B_RESULT'
               OR NEW.result_summary IS DISTINCT FROM jsonb_build_object(
                    'identity_schema', 'systematic_fx.m0b.result_summary.v1',
                    'epoch_sha256', candidate_record.epoch_sha256,
                    'candidate_sha256', candidate_record.candidate_sha256,
                    'result_artifact_id', NEW.result_artifact_id,
                    'result_sha256', result_artifact_record.sha256,
                    'data_role', 'SEARCH',
                    'classification', decision_record.classification,
                    'admission_rules_sha256', candidate_record.admission_rules_sha256,
                    'terminal_metrics_sha256', decision_record.metrics_sha256) THEN
                RAISE EXCEPTION 'M0b SUCCEEDED attempt result summary differs from numeric evidence';
            END IF;
        END IF;
    END IF;
    IF TG_OP = 'INSERT' THEN
        SELECT count(*)::integer INTO attempt_count
          FROM systematic_fx.research_run_attempts
         WHERE research_run_spec_id = NEW.research_run_spec_id;
        IF NEW.attempt_number <> attempt_count + 1
           OR NEW.attempt_number > candidate_record.max_attempts_per_candidate
           OR NEW.status <> 'QUEUED' OR NEW.reused_attempt_id IS NOT NULL
           OR NEW.started_at IS NOT NULL OR NEW.finished_at IS NOT NULL
           OR NEW.error_message IS NOT NULL OR NEW.result_artifact_id IS NOT NULL
           OR NEW.trade_ledger_artifact_id IS NOT NULL OR NEW.result_summary <> '{}'::jsonb
           OR EXISTS (
               SELECT 1 FROM systematic_fx.research_run_attempts
                WHERE research_run_spec_id = NEW.research_run_spec_id
                  AND status IN ('QUEUED', 'RUNNING', 'SUCCEEDED')) THEN
            RAISE EXCEPTION 'M0b retry requires prior terminal failures and one pristine queue';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION systematic_fx.require_m0b_numeric_terminal_decision()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE decision_count integer;
BEGIN
    IF NEW.status NOT IN ('SCREENED_OUT', 'REGISTERED') THEN RETURN NEW; END IF;
    SELECT count(*)::integer INTO decision_count
      FROM systematic_fx.m0b_admission_decisions AS decision
      JOIN systematic_fx.research_run_attempts AS attempt
        USING (research_run_attempt_id)
     WHERE decision.m0b_candidate_id = NEW.m0b_candidate_id
       AND decision.classification = NEW.status
       AND attempt.research_run_spec_id = NEW.research_run_spec_id
       AND attempt.status = 'SUCCEEDED';
    IF decision_count <> 1 THEN
        RAISE EXCEPTION 'M0b terminal candidate requires one derived numeric admission decision';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER m0b_candidates_require_numeric_decision
AFTER INSERT OR UPDATE ON systematic_fx.m0b_candidates
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION systematic_fx.require_m0b_numeric_terminal_decision();

CREATE FUNCTION systematic_fx.validate_m0b_worker_lease()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE candidate_spec bigint; attempt_spec bigint; candidate_status text; attempt_status text;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF ROW(NEW.research_run_attempt_id, NEW.m0b_candidate_id, NEW.worker_id,
               NEW.login_role, NEW.lease_token_sha256, NEW.created_at)
           IS DISTINCT FROM
           ROW(OLD.research_run_attempt_id, OLD.m0b_candidate_id, OLD.worker_id,
               OLD.login_role, OLD.lease_token_sha256, OLD.created_at) THEN
            RAISE EXCEPTION 'M0b worker lease permits only one release transition';
        END IF;
        IF OLD.status = 'ACTIVE' AND NEW.status = 'ACTIVE'
           AND NEW.released_at IS NULL
           AND NEW.leased_until >= OLD.leased_until
           AND NEW.leased_until <= statement_timestamp() + interval '1 hour' THEN
            RETURN NEW;
        END IF;
        IF OLD.status <> 'ACTIVE' OR NEW.status <> 'RELEASED'
           OR NEW.released_at IS NULL
           OR NEW.leased_until IS DISTINCT FROM OLD.leased_until THEN
            RAISE EXCEPTION 'M0b worker lease permits only renewal or one release';
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'M0b worker leases are append-preserved';
    END IF;
    SELECT research_run_spec_id, status INTO STRICT candidate_spec, candidate_status
      FROM systematic_fx.m0b_candidates
     WHERE m0b_candidate_id = NEW.m0b_candidate_id FOR SHARE;
    SELECT research_run_spec_id, status INTO STRICT attempt_spec, attempt_status
      FROM systematic_fx.research_run_attempts
     WHERE research_run_attempt_id = NEW.research_run_attempt_id FOR SHARE;
    IF candidate_spec <> attempt_spec OR candidate_status <> 'RUNNING'
       OR attempt_status <> 'RUNNING' OR NEW.status <> 'ACTIVE'
       OR NEW.released_at IS NOT NULL THEN
        RAISE EXCEPTION 'M0b worker lease must bind one active candidate attempt';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER m0b_worker_leases_validate
BEFORE INSERT OR UPDATE OR DELETE ON systematic_fx.m0b_worker_leases
FOR EACH ROW EXECUTE FUNCTION systematic_fx.validate_m0b_worker_lease();

CREATE FUNCTION systematic_fx.m0b_worker_authorized()
RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT pg_has_role(session_user, 'systematic_fx_m0b_worker', 'MEMBER')
       AND NOT EXISTS (
           SELECT 1 FROM pg_roles
            WHERE rolname = session_user
              AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls));
$$;

CREATE FUNCTION systematic_fx.m0b_worker_claim_next(
    target_epoch_key text,
    target_worker_id text,
    target_lease_token_sha256 text,
    lease_seconds integer DEFAULT 300)
RETURNS TABLE (
    m0b_candidate_id bigint,
    research_run_attempt_id bigint,
    attempt_number integer,
    candidate_sha256 text,
    candidate_kind text,
    canonical_candidate jsonb,
    epoch_sha256 text,
    work_spec_sha256 text,
    work_spec_byte_size bigint,
    attempt_status text,
    lease_status text,
    leased_until timestamptz)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $$
DECLARE target record; stale record; new_attempt_id bigint; new_attempt_number integer;
        target_leased_until timestamptz; recovery_time timestamptz;
BEGIN
    IF NOT systematic_fx.m0b_worker_authorized()
       OR btrim(COALESCE(target_epoch_key, '')) = ''
       OR btrim(COALESCE(target_worker_id, '')) = ''
       OR length(target_worker_id) > 128
       OR target_lease_token_sha256 !~ '^[0-9a-f]{64}$'
       OR lease_seconds NOT BETWEEN 30 AND 3600 THEN
        RAISE EXCEPTION 'unauthorized or invalid M0b worker claim';
    END IF;
    -- Exact replay after a lost claim response returns the same active lease;
    -- a token can never be rebound to another candidate or worker.
    SELECT candidate.m0b_candidate_id, candidate.research_run_spec_id,
           candidate.candidate_sha256, candidate.candidate_kind,
           candidate.canonical_candidate, attempt.attempt_number,
           lease.research_run_attempt_id, lease.leased_until, lease.worker_id,
           lease.login_role,
           epoch.epoch_key, epoch.epoch_sha256,
           work_artifact.sha256 AS work_spec_sha256,
           work_artifact.byte_size AS work_spec_byte_size,
           attempt.status AS attempt_status, lease.status AS lease_status
      INTO target
      FROM systematic_fx.m0b_worker_leases lease
      JOIN systematic_fx.m0b_candidates candidate USING (m0b_candidate_id)
      JOIN systematic_fx.research_run_attempts attempt USING (research_run_attempt_id)
      JOIN systematic_fx.m0b_epochs epoch USING (m0b_epoch_id)
     JOIN systematic_fx.artifacts work_artifact
        ON work_artifact.artifact_id = candidate.work_artifact_id
     WHERE lease.lease_token_sha256 = target_lease_token_sha256
     FOR UPDATE OF lease;
    IF FOUND THEN
        IF target.worker_id <> target_worker_id
           OR target.login_role <> session_user
           OR target.epoch_key <> target_epoch_key THEN
            RAISE EXCEPTION 'M0b worker lease token replay identity drifted';
        END IF;
        -- The persisted exact token is the durable ownership record.  Its same
        -- authenticated LOGIN/worker replay renews even after wall-clock
        -- expiry; only a different fresh token may invoke stale recovery.
        IF target.lease_status = 'ACTIVE'
           AND target.attempt_status = 'RUNNING' THEN
            UPDATE systematic_fx.m0b_worker_leases AS replayed_lease
               SET leased_until = greatest(
                   replayed_lease.leased_until,
                   statement_timestamp() + make_interval(secs => lease_seconds))
             WHERE replayed_lease.research_run_attempt_id =
                   target.research_run_attempt_id
            RETURNING replayed_lease.leased_until INTO target.leased_until;
        END IF;
        RETURN QUERY SELECT target.m0b_candidate_id,
                            target.research_run_attempt_id, target.attempt_number,
                            target.candidate_sha256, target.candidate_kind,
                            target.canonical_candidate, target.epoch_sha256,
                            target.work_spec_sha256, target.work_spec_byte_size,
                            target.attempt_status, target.lease_status,
                            target.leased_until;
        RETURN;
    END IF;
    -- A dead worker may leave one RUNNING attempt behind.  Recover only after
    -- its precommitted lease expires; the attempt becomes an ordinary FAILED
    -- retry and the candidate never receives more than the frozen attempt cap.
    SELECT candidate.m0b_candidate_id, candidate.research_run_spec_id,
           attempt.research_run_attempt_id, attempt.attempt_number,
           epoch.max_attempts_per_candidate
      INTO stale
      FROM systematic_fx.m0b_worker_leases lease
      JOIN systematic_fx.research_run_attempts attempt USING (research_run_attempt_id)
      JOIN systematic_fx.m0b_candidates candidate USING (m0b_candidate_id)
      JOIN systematic_fx.m0b_epochs epoch USING (m0b_epoch_id)
      JOIN systematic_fx.campaigns campaign USING (campaign_id)
     WHERE epoch.epoch_key = target_epoch_key
       AND epoch.status = 'RUNNING' AND campaign.status = 'RUNNING'
       AND campaign.holdout_revealed_at IS NULL AND campaign.closed_at IS NULL
       AND lease.status = 'ACTIVE' AND lease.leased_until < statement_timestamp()
       AND attempt.status = 'RUNNING' AND candidate.status = 'RUNNING'
       AND NOT EXISTS (
            SELECT 1
              FROM systematic_fx.m0b_checkpoints checkpoint
             WHERE checkpoint.m0b_candidate_id = candidate.m0b_candidate_id
               AND checkpoint.research_run_attempt_id = attempt.research_run_attempt_id
               AND checkpoint.cursor #>> '{state,complete}' = 'true')
     ORDER BY lease.leased_until, lease.research_run_attempt_id
     FOR UPDATE OF lease, attempt, candidate SKIP LOCKED LIMIT 1;
    IF FOUND THEN
        recovery_time := statement_timestamp();
        UPDATE systematic_fx.research_run_attempts AS recovered_attempt
           SET status = 'FAILED', finished_at = recovery_time,
               error_message = 'M0b worker lease expired before completion'
         WHERE recovered_attempt.research_run_attempt_id = stale.research_run_attempt_id;
        UPDATE systematic_fx.m0b_worker_leases AS recovered_lease
           SET status = 'RELEASED', released_at = recovery_time,
               failure_retryable = stale.attempt_number
                   < stale.max_attempts_per_candidate,
               failure_resulting_status = CASE
                   WHEN stale.attempt_number < stale.max_attempts_per_candidate
                   THEN 'RUNNING' ELSE 'FAILED' END
         WHERE recovered_lease.research_run_attempt_id = stale.research_run_attempt_id;
        IF stale.attempt_number >= stale.max_attempts_per_candidate THEN
            UPDATE systematic_fx.m0b_candidates
               SET status = 'CRASHED', finished_at = recovery_time,
                   error_message = 'M0b worker retry budget exhausted after expired lease'
             WHERE m0b_candidate_id = stale.m0b_candidate_id;
        END IF;
    END IF;
    SELECT candidate.m0b_candidate_id, candidate.research_run_spec_id,
           candidate.candidate_sha256, candidate.candidate_kind,
           candidate.canonical_candidate, candidate.status,
           epoch.max_attempts_per_candidate, epoch.epoch_sha256,
           work_artifact.sha256 AS work_spec_sha256,
           work_artifact.byte_size AS work_spec_byte_size
      INTO target
      FROM systematic_fx.m0b_candidates AS candidate
      JOIN systematic_fx.m0b_epochs AS epoch USING (m0b_epoch_id)
      JOIN systematic_fx.campaigns AS campaign USING (campaign_id)
      JOIN systematic_fx.artifacts AS work_artifact
        ON work_artifact.artifact_id = candidate.work_artifact_id
     WHERE epoch.epoch_key = target_epoch_key
       AND epoch.status = 'RUNNING' AND campaign.status = 'RUNNING'
       AND campaign.holdout_revealed_at IS NULL AND campaign.closed_at IS NULL
       AND (
          candidate.status = 'QUEUED'
          OR (candidate.status = 'RUNNING'
              AND NOT EXISTS (
                  SELECT 1 FROM systematic_fx.research_run_attempts active_attempt
                   WHERE active_attempt.research_run_spec_id = candidate.research_run_spec_id
                     AND active_attempt.status IN ('QUEUED', 'RUNNING', 'SUCCEEDED'))
              AND (SELECT count(*) FROM systematic_fx.research_run_attempts prior_attempt
                    WHERE prior_attempt.research_run_spec_id = candidate.research_run_spec_id)
                    < epoch.max_attempts_per_candidate)
       )
     ORDER BY CASE candidate.candidate_kind WHEN 'REAL' THEN 0 ELSE 1 END,
              candidate.ordinal, candidate.m0b_candidate_id
     FOR UPDATE OF candidate SKIP LOCKED LIMIT 1;
    IF NOT FOUND THEN RETURN; END IF;
    IF target.status = 'QUEUED' THEN
        UPDATE systematic_fx.m0b_candidates
           SET status = 'RUNNING', started_at = statement_timestamp()
         WHERE systematic_fx.m0b_candidates.m0b_candidate_id = target.m0b_candidate_id;
    END IF;
    SELECT count(*)::integer + 1 INTO new_attempt_number
      FROM systematic_fx.research_run_attempts
     WHERE research_run_spec_id = target.research_run_spec_id;
    INSERT INTO systematic_fx.research_run_attempts
        (research_run_spec_id, attempt_number)
    VALUES (target.research_run_spec_id, new_attempt_number)
    RETURNING systematic_fx.research_run_attempts.research_run_attempt_id
         INTO new_attempt_id;
    UPDATE systematic_fx.research_run_attempts
       SET status = 'RUNNING', started_at = statement_timestamp()
     WHERE systematic_fx.research_run_attempts.research_run_attempt_id = new_attempt_id;
    target_leased_until := statement_timestamp() + make_interval(secs => lease_seconds);
    INSERT INTO systematic_fx.m0b_worker_leases
        (research_run_attempt_id, m0b_candidate_id, login_role, worker_id,
         lease_token_sha256, leased_until)
    VALUES (new_attempt_id, target.m0b_candidate_id, session_user, target_worker_id,
            target_lease_token_sha256, target_leased_until);
    RETURN QUERY SELECT target.m0b_candidate_id, new_attempt_id, new_attempt_number,
                        target.candidate_sha256, target.candidate_kind,
                        target.canonical_candidate, target.epoch_sha256,
                        target.work_spec_sha256, target.work_spec_byte_size,
                        'RUNNING'::text, 'ACTIVE'::text,
                        target_leased_until;
END;
$$;

CREATE FUNCTION systematic_fx.m0b_worker_checkpoint(
    target_candidate_id bigint,
    target_attempt_id bigint,
    target_lease_token_sha256 text,
    target_sequence integer,
    target_checkpoint_sha256 text,
    target_predecessor_sha256 text,
    target_cursor jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $$
DECLARE new_checkpoint_id bigint; existing record;
BEGIN
    IF NOT systematic_fx.m0b_worker_authorized()
       OR target_sequence NOT BETWEEN 1 AND 100000
       OR target_checkpoint_sha256 !~ '^[0-9a-f]{64}$'
       OR (target_predecessor_sha256 IS NOT NULL
           AND target_predecessor_sha256 !~ '^[0-9a-f]{64}$')
       OR jsonb_typeof(target_cursor) IS DISTINCT FROM 'object'
       OR pg_column_size(target_cursor) > 1048576
       OR NOT EXISTS (
        SELECT 1 FROM systematic_fx.m0b_worker_leases lease
         WHERE lease.m0b_candidate_id = target_candidate_id
           AND lease.research_run_attempt_id = target_attempt_id
           AND lease.login_role = session_user
           AND lease.lease_token_sha256 = target_lease_token_sha256) THEN
        RAISE EXCEPTION 'M0b worker lease identity is absent';
    END IF;
    SELECT m0b_checkpoint_id, m0b_candidate_id, checkpoint_sha256,
           predecessor_sha256, cursor
      INTO existing
      FROM systematic_fx.m0b_checkpoints
     WHERE research_run_attempt_id = target_attempt_id
       AND checkpoint_sequence = target_sequence;
    IF FOUND THEN
        IF existing.m0b_candidate_id = target_candidate_id
           AND existing.checkpoint_sha256 = target_checkpoint_sha256
           AND existing.predecessor_sha256 IS NOT DISTINCT FROM target_predecessor_sha256
           AND existing.cursor = target_cursor THEN
            RETURN existing.m0b_checkpoint_id;
        END IF;
        RAISE EXCEPTION 'M0b checkpoint replay identity drifted';
    END IF;
    IF NOT systematic_fx.m0b_worker_authorized() OR NOT EXISTS (
        SELECT 1 FROM systematic_fx.m0b_worker_leases lease
         WHERE lease.m0b_candidate_id = target_candidate_id
           AND lease.research_run_attempt_id = target_attempt_id
           AND lease.login_role = session_user
           AND lease.lease_token_sha256 = target_lease_token_sha256
           AND lease.status = 'ACTIVE' AND lease.leased_until >= statement_timestamp()
         FOR UPDATE) THEN
        RAISE EXCEPTION 'M0b worker lease is absent or expired';
    END IF;
    INSERT INTO systematic_fx.m0b_checkpoints
        (m0b_candidate_id, research_run_attempt_id, checkpoint_sequence,
         checkpoint_sha256, predecessor_sha256, cursor)
    VALUES (target_candidate_id, target_attempt_id, target_sequence,
            target_checkpoint_sha256, target_predecessor_sha256, target_cursor)
    RETURNING m0b_checkpoint_id INTO new_checkpoint_id;
    UPDATE systematic_fx.m0b_worker_leases
       SET leased_until = greatest(
           leased_until, statement_timestamp() + interval '5 minutes')
     WHERE research_run_attempt_id = target_attempt_id AND status = 'ACTIVE';
    RETURN new_checkpoint_id;
END;
$$;

CREATE FUNCTION systematic_fx.m0b_worker_terminalize(
    target_candidate_id bigint,
    target_attempt_id bigint,
    target_lease_token_sha256 text,
    target_result_sha256 text,
    target_result_byte_size bigint,
    target_metrics jsonb)
RETURNS TABLE (artifact_id bigint, classification text, registered_at timestamptz)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $$
DECLARE candidate_record record; rules_sha256 text; metrics_sha256 text;
        derived_classification text; new_artifact_id bigint; terminal_time timestamptz;
        result_metadata jsonb; derived_summary jsonb; result_uri text; result_key text;
        existing_terminal record; latest_checkpoint record; lease_record record;
BEGIN
    IF NOT systematic_fx.m0b_worker_authorized()
       OR target_result_sha256 !~ '^[0-9a-f]{64}$'
       OR target_result_byte_size NOT BETWEEN 1 AND 67108864
       OR NOT systematic_fx.m0b_numeric_metrics_valid(target_metrics) THEN
        RAISE EXCEPTION 'invalid M0b worker result identity or metrics';
    END IF;
    -- Serialize terminalization ahead of the latest-checkpoint read.  The
    -- checkpoint capability locks this same row before append, so a terminal
    -- decision can never race against a later cursor from the same attempt.
    SELECT lease.status, lease.leased_until
      INTO lease_record
      FROM systematic_fx.m0b_worker_leases lease
         WHERE lease.m0b_candidate_id = target_candidate_id
           AND lease.research_run_attempt_id = target_attempt_id
           AND lease.login_role = session_user
           AND lease.lease_token_sha256 = target_lease_token_sha256
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'M0b worker lease identity is absent';
    END IF;
    SELECT checkpoint.checkpoint_sequence,
           checkpoint.cursor #>> '{state,complete}' AS complete,
           checkpoint.cursor #> '{state,result_artifact}' AS result_identity
      INTO latest_checkpoint
      FROM systematic_fx.m0b_checkpoints AS checkpoint
     WHERE checkpoint.m0b_candidate_id = target_candidate_id
       AND checkpoint.research_run_attempt_id = target_attempt_id
     ORDER BY checkpoint.checkpoint_sequence DESC
     LIMIT 1;
    IF NOT FOUND
       OR latest_checkpoint.complete IS DISTINCT FROM 'true'
       OR jsonb_typeof(latest_checkpoint.result_identity) IS DISTINCT FROM 'object'
       OR latest_checkpoint.result_identity #>> '{content_sha256}'
            IS DISTINCT FROM target_result_sha256
       OR (latest_checkpoint.result_identity #>> '{byte_size}')::bigint
            IS DISTINCT FROM target_result_byte_size
       OR latest_checkpoint.result_identity -> 'metrics'
            IS DISTINCT FROM target_metrics THEN
        RAISE EXCEPTION 'M0b terminal result differs from latest complete checkpoint';
    END IF;
    SELECT decision.result_artifact_id, decision.classification,
           candidate.registered_at, artifact.sha256, artifact.byte_size,
           decision.metrics
      INTO existing_terminal
      FROM systematic_fx.m0b_admission_decisions decision
      JOIN systematic_fx.m0b_candidates candidate USING (m0b_candidate_id)
      JOIN systematic_fx.artifacts artifact
        ON artifact.artifact_id = decision.result_artifact_id
     WHERE decision.m0b_candidate_id = target_candidate_id
       AND decision.research_run_attempt_id = target_attempt_id;
    IF FOUND THEN
        IF existing_terminal.sha256 = target_result_sha256
           AND existing_terminal.byte_size = target_result_byte_size
           AND existing_terminal.metrics = target_metrics
           AND latest_checkpoint.result_identity #>> '{classification}'
                = existing_terminal.classification THEN
            RETURN QUERY SELECT existing_terminal.result_artifact_id,
                                existing_terminal.classification,
                                existing_terminal.registered_at;
            RETURN;
        END IF;
        RAISE EXCEPTION 'M0b terminal replay identity drifted';
    END IF;
    IF lease_record.status <> 'ACTIVE'
       OR lease_record.leased_until < statement_timestamp() THEN
        RAISE EXCEPTION 'M0b worker lease is absent or expired';
    END IF;
    SELECT candidate.candidate_kind, candidate.candidate_sha256,
           candidate.research_run_spec_id, candidate.status AS candidate_status,
           attempt.status AS attempt_status,
           epoch.m0b_epoch_id, epoch.epoch_sha256,
           epoch.canonical_epoch -> 'admission_rules' AS rules
      INTO STRICT candidate_record
      FROM systematic_fx.m0b_candidates candidate
      JOIN systematic_fx.m0b_epochs epoch USING (m0b_epoch_id)
      JOIN systematic_fx.research_run_attempts attempt
        ON attempt.research_run_attempt_id = target_attempt_id
     WHERE candidate.m0b_candidate_id = target_candidate_id
     FOR UPDATE OF candidate, attempt;
    IF candidate_record.candidate_status <> 'RUNNING'
       OR candidate_record.attempt_status <> 'RUNNING'
       OR (SELECT research_run_spec_id FROM systematic_fx.research_run_attempts
            WHERE research_run_attempt_id = target_attempt_id)
            <> candidate_record.research_run_spec_id THEN
        RAISE EXCEPTION 'M0b worker terminal target is not the active candidate attempt';
    END IF;
    rules_sha256 := systematic_fx.canonical_jsonb_sha256(candidate_record.rules);
    metrics_sha256 := systematic_fx.canonical_jsonb_sha256(target_metrics);
    derived_classification := CASE
        WHEN candidate_record.candidate_kind = 'REAL'
         AND systematic_fx.m0b_numeric_metrics_admitted(candidate_record.rules, target_metrics)
        THEN 'REGISTERED' ELSE 'SCREENED_OUT' END;
    IF latest_checkpoint.result_identity #>> '{classification}'
           IS DISTINCT FROM derived_classification THEN
        RAISE EXCEPTION 'M0b checkpoint classification differs from immutable numeric rules';
    END IF;
    result_key := format('m0b-worker-result:%s:%s:%s',
                         target_candidate_id, target_attempt_id, target_result_sha256);
    result_uri := format('m0b-result://search/%s/%s/%s/sha256=%s.json',
                         candidate_record.m0b_epoch_id, target_candidate_id,
                         target_attempt_id, target_result_sha256);
    result_metadata := jsonb_build_object(
        'identity_schema', 'systematic_fx.m0b.result.v1',
        'epoch_sha256', candidate_record.epoch_sha256,
        'm0b_epoch_id', candidate_record.m0b_epoch_id,
        'candidate_sha256', candidate_record.candidate_sha256,
        'm0b_candidate_id', target_candidate_id,
        'research_run_attempt_id', target_attempt_id,
        'result_sha256', target_result_sha256,
        'admission_rules_sha256', rules_sha256);
    INSERT INTO systematic_fx.artifacts
        (artifact_key, artifact_type, uri, sha256, byte_size, media_type, metadata)
    VALUES (result_key, 'M0B_RESULT', result_uri, target_result_sha256,
            target_result_byte_size, 'application/json', result_metadata)
    RETURNING systematic_fx.artifacts.artifact_id INTO new_artifact_id;
    INSERT INTO systematic_fx.m0b_admission_decisions
        (m0b_candidate_id, research_run_attempt_id, result_artifact_id,
         admission_rules_sha256, metrics, metrics_sha256, classification)
    VALUES (target_candidate_id, target_attempt_id, new_artifact_id,
            rules_sha256, target_metrics, metrics_sha256, derived_classification);
    derived_summary := jsonb_build_object(
        'identity_schema', 'systematic_fx.m0b.result_summary.v1',
        'epoch_sha256', candidate_record.epoch_sha256,
        'candidate_sha256', candidate_record.candidate_sha256,
        'result_artifact_id', new_artifact_id,
        'result_sha256', target_result_sha256,
        'data_role', 'SEARCH',
        'classification', derived_classification,
        'admission_rules_sha256', rules_sha256,
        'terminal_metrics_sha256', metrics_sha256);
    terminal_time := statement_timestamp();
    UPDATE systematic_fx.research_run_attempts
       SET status = 'SUCCEEDED', result_artifact_id = new_artifact_id,
           result_summary = derived_summary, finished_at = terminal_time
     WHERE research_run_attempt_id = target_attempt_id;
    INSERT INTO systematic_fx.m0b_artifact_links
        (m0b_candidate_id, research_run_attempt_id, artifact_id,
         artifact_role, artifact_sha256, artifact_byte_size)
    VALUES (target_candidate_id, target_attempt_id, new_artifact_id,
            'RESULT', target_result_sha256, target_result_byte_size);
    UPDATE systematic_fx.m0b_candidates
       SET status = derived_classification, finished_at = terminal_time,
           registered_at = CASE WHEN derived_classification = 'REGISTERED'
                                THEN terminal_time ELSE NULL END
     WHERE m0b_candidate_id = target_candidate_id;
    UPDATE systematic_fx.m0b_worker_leases
       SET status = 'RELEASED', released_at = terminal_time
     WHERE research_run_attempt_id = target_attempt_id;
    RETURN QUERY SELECT new_artifact_id, derived_classification,
                        CASE WHEN derived_classification = 'REGISTERED'
                             THEN terminal_time ELSE NULL END;
END;
$$;

CREATE FUNCTION systematic_fx.m0b_worker_fail(
    target_candidate_id bigint,
    target_attempt_id bigint,
    target_lease_token_sha256 text,
    target_error_message text,
    retryable boolean DEFAULT true)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $$
DECLARE candidate_record record; terminal_time timestamptz; resulting_status text;
BEGIN
    IF NOT systematic_fx.m0b_worker_authorized()
       OR btrim(COALESCE(target_error_message, '')) = ''
       OR length(target_error_message) > 4000 THEN
        RAISE EXCEPTION 'unauthorized or invalid M0b worker failure';
    END IF;
    SELECT candidate.research_run_spec_id, candidate.status,
           epoch.max_attempts_per_candidate, attempt.attempt_number,
           attempt.status AS attempt_status, attempt.error_message,
           lease.status AS lease_status, lease.leased_until,
           lease.failure_retryable, lease.failure_resulting_status
      INTO STRICT candidate_record
      FROM systematic_fx.m0b_candidates candidate
      JOIN systematic_fx.m0b_epochs epoch USING (m0b_epoch_id)
      JOIN systematic_fx.research_run_attempts attempt
        ON attempt.research_run_attempt_id = target_attempt_id
      JOIN systematic_fx.m0b_worker_leases lease
        ON lease.research_run_attempt_id = target_attempt_id
       AND lease.m0b_candidate_id = candidate.m0b_candidate_id
       AND lease.login_role = session_user
       AND lease.lease_token_sha256 = target_lease_token_sha256
     WHERE candidate.m0b_candidate_id = target_candidate_id
     FOR UPDATE OF candidate, attempt, lease;
    IF candidate_record.attempt_status = 'FAILED'
       AND candidate_record.lease_status = 'RELEASED' THEN
        IF candidate_record.error_message IS DISTINCT FROM target_error_message THEN
            RAISE EXCEPTION 'M0b failure replay identity drifted';
        END IF;
        IF candidate_record.failure_retryable IS DISTINCT FROM retryable THEN
            RAISE EXCEPTION 'M0b failure replay retry policy drifted';
        END IF;
        RETURN candidate_record.failure_resulting_status;
    END IF;
    IF candidate_record.lease_status <> 'ACTIVE'
       OR candidate_record.leased_until < statement_timestamp() THEN
        RAISE EXCEPTION 'unauthorized or invalid M0b worker failure';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM systematic_fx.m0b_checkpoints checkpoint
         WHERE checkpoint.m0b_candidate_id = target_candidate_id
           AND checkpoint.research_run_attempt_id = target_attempt_id
           AND checkpoint.cursor #>> '{state,complete}' = 'true') THEN
        RAISE EXCEPTION 'M0b complete checkpoint must terminalize, not fail';
    END IF;
    IF candidate_record.status <> 'RUNNING'
       OR candidate_record.attempt_status <> 'RUNNING' THEN
        RAISE EXCEPTION 'M0b worker failure target is not running';
    END IF;
    terminal_time := statement_timestamp();
    UPDATE systematic_fx.research_run_attempts
       SET status = 'FAILED', finished_at = terminal_time,
           error_message = target_error_message
     WHERE research_run_attempt_id = target_attempt_id;
    resulting_status := CASE
        WHEN retryable AND candidate_record.attempt_number
             < candidate_record.max_attempts_per_candidate
        THEN 'RUNNING' ELSE 'FAILED' END;
    IF resulting_status = 'FAILED' THEN
        UPDATE systematic_fx.m0b_candidates
           SET status = 'FAILED', finished_at = terminal_time,
               error_message = target_error_message
         WHERE m0b_candidate_id = target_candidate_id;
    END IF;
    UPDATE systematic_fx.m0b_worker_leases
       SET status = 'RELEASED', released_at = terminal_time,
           failure_retryable = retryable,
           failure_resulting_status = resulting_status
     WHERE research_run_attempt_id = target_attempt_id;
    RETURN resulting_status;
END;
$$;

-- Trigger guards execute as the outer API owner's current_user.  Keep them
-- SECURITY INVOKER so unrelated bar/Phase1A workflows never transitively run
-- with a privileged migration owner.  Direct EXECUTE stays revoked; trigger
-- dispatch itself does not require it.
ALTER FUNCTION systematic_fx.validate_m0b_candidate_update_context() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.protect_m0b_attempt_lifecycle() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.validate_m0b_checkpoint_insert() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.validate_m0b_admission_decision_insert() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.validate_m0b_artifact_link_insert() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.validate_m0b_candidate_terminal() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.validate_m0b_candidate_failure() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.require_m0b_success_candidate_pair() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.require_m0b_numeric_terminal_decision() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.validate_m0b_worker_lease() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.enforce_bar_state_v2a_predecessor_attempt_freeze()
    SECURITY INVOKER;
ALTER FUNCTION systematic_fx.enforce_bar_state_attempt_lifecycle() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.enforce_bar_pattern_attempt_immediate() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.protect_phase1a_attempt_artifact_links() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.protect_research_run_attempt_history() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.protect_research_run_attempt_history()
    SET search_path = pg_catalog;
ALTER FUNCTION systematic_fx.require_duplicate_skip_success() SECURITY INVOKER;
-- These pre-M0b statement/deferred triggers also fire on the generic attempt
-- and artifact rows written by the worker API.  Keep their existing narrowly
-- scoped validation/publication behavior as SECURITY INVOKER.  During an API
-- statement they use the dedicated API owner's exact dependency grants; they
-- never grant direct DML to the LOGIN or change unrelated workflow authority.
ALTER FUNCTION systematic_fx.request_publication_refresh() SECURITY INVOKER;
ALTER FUNCTION systematic_fx.request_publication_refresh()
    SET search_path = pg_catalog;
ALTER FUNCTION systematic_fx.require_phase1a_ordered_outcome_attempt_manifest()
    SECURITY INVOKER;
ALTER FUNCTION systematic_fx.require_bar_pattern_terminal_consistency()
    SECURITY INVOKER;
ALTER FUNCTION systematic_fx.require_bar_state_terminal_pair()
    SECURITY INVOKER;
REVOKE ALL ON FUNCTION systematic_fx.validate_m0b_candidate_update_context() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.validate_m0b_epoch_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.validate_m0b_candidate_work_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.protect_m0b_candidate() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.protect_m0b_run_spec_lineage() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.protect_m0b_attempt_lifecycle() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.validate_m0b_checkpoint_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.validate_m0b_admission_decision_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.validate_m0b_artifact_link_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.validate_m0b_candidate_terminal() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.validate_m0b_candidate_failure() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.require_m0b_success_candidate_pair() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.require_m0b_numeric_terminal_decision() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.validate_m0b_worker_lease() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.enforce_bar_state_v2a_predecessor_attempt_freeze()
    FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.enforce_bar_state_attempt_lifecycle() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.enforce_bar_pattern_attempt_immediate() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.protect_phase1a_attempt_artifact_links() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.protect_research_run_attempt_history() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.require_duplicate_skip_success() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.request_publication_refresh() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.require_phase1a_ordered_outcome_attempt_manifest()
    FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.require_bar_pattern_terminal_consistency()
    FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.require_bar_state_terminal_pair() FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.m0b_numeric_admission_rules_valid(jsonb)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.m0b_numeric_metrics_valid(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.m0b_numeric_metrics_admitted(jsonb, jsonb)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.m0b_worker_checkpoint_state_valid(jsonb, jsonb)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.m0b_candidate_work_barrier_matches(jsonb, jsonb)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.m0b_worker_authorized() FROM PUBLIC;

REVOKE ALL ON FUNCTION systematic_fx.m0b_worker_claim_next(text, text, text, integer)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.m0b_worker_checkpoint(
    bigint, bigint, text, integer, text, text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.m0b_worker_terminalize(
    bigint, bigint, text, text, bigint, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION systematic_fx.m0b_worker_fail(
    bigint, bigint, text, text, boolean) FROM PUBLIC;

COMMENT ON TABLE systematic_fx.m0b_admission_decisions IS
    'Append-only DB-derived M0b SEARCH admission result; maximum authority REGISTER.';
COMMENT ON TABLE systematic_fx.m0b_worker_leases IS
    'Opaque-token capability binding for least-privilege M0b worker APIs.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (30, 'm0b_numeric_admission_worker_api', :'migration_checksum');

COMMIT;
