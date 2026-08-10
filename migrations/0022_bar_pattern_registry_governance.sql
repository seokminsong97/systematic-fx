BEGIN;

CREATE FUNCTION systematic_fx.canonical_jsonb_text(target jsonb)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $$
    SELECT CASE jsonb_typeof(target)
        WHEN 'object' THEN
            '{' || COALESCE(
                (
                    SELECT string_agg(
                        to_jsonb(item.key)::text || ':' ||
                            systematic_fx.canonical_jsonb_text(item.value),
                        ',' ORDER BY item.key COLLATE "C"
                    )
                    FROM jsonb_each(target) AS item
                ),
                ''
            ) || '}'
        WHEN 'array' THEN
            '[' || COALESCE(
                (
                    SELECT string_agg(
                        systematic_fx.canonical_jsonb_text(item.value),
                        ',' ORDER BY item.ordinality
                    )
                    FROM jsonb_array_elements(target)
                        WITH ORDINALITY AS item(value, ordinality)
                ),
                ''
            ) || ']'
        ELSE target::text
    END;
$$;

CREATE FUNCTION systematic_fx.canonical_jsonb_sha256(target jsonb)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $$
    SELECT encode(
        sha256(convert_to(systematic_fx.canonical_jsonb_text(target), 'UTF8')),
        'hex'
    );
$$;

CREATE FUNCTION systematic_fx.jsonb_has_exact_keys(
    target jsonb,
    expected_keys text[]
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $$
    SELECT jsonb_typeof(target) = 'object'
       AND ARRAY(
               SELECT key
               FROM jsonb_object_keys(target) AS key
               ORDER BY key COLLATE "C"
           ) = ARRAY(
               SELECT key
               FROM unnest(expected_keys) AS key
               ORDER BY key COLLATE "C"
           );
$$;

CREATE FUNCTION systematic_fx.bar_pattern_experiment_is_governed(
    target_experiment_id bigint
)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM systematic_fx.experiments AS experiment
        JOIN systematic_fx.campaigns AS campaign
          ON campaign.campaign_id = experiment.campaign_id
        WHERE experiment.experiment_id = target_experiment_id
          AND experiment.experiment_key =
              'bar_pattern_discovery_v1:experiment:frozen_candidate_catalog:v1'
          AND campaign.campaign_key = 'bar_pattern_discovery_v1'
    );
$$;

CREATE FUNCTION systematic_fx.bar_pattern_run_spec_matches_trial(
    target_spec_id bigint,
    target_trial_id bigint
)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
    WITH bound AS (
        SELECT 1
        FROM systematic_fx.experiment_trials AS trial
        JOIN systematic_fx.experiments AS experiment
          ON experiment.experiment_id = trial.experiment_id
        JOIN systematic_fx.campaigns AS campaign
          ON campaign.campaign_id = experiment.campaign_id
        JOIN systematic_fx.datasets AS dataset
          ON dataset.dataset_id = campaign.dataset_id
        JOIN systematic_fx.artifacts AS registration_artifact
          ON registration_artifact.artifact_id = experiment.registration_artifact_id
        JOIN systematic_fx.research_run_specs AS run_spec
          ON run_spec.research_run_spec_id = target_spec_id
        CROSS JOIN LATERAL (
            SELECT trial.parameters -> 'campaign_definition' AS campaign_definition,
                   trial.parameters -> 'candidate_definition' AS candidate_definition
        ) AS definitions
        CROSS JOIN LATERAL (
            SELECT definitions.campaign_definition -> 'entry' AS campaign_entry,
                   definitions.campaign_definition -> 'barriers' AS campaign_barriers,
                   definitions.campaign_definition -> 'bars' AS campaign_bars,
                   definitions.campaign_definition -> 'market' AS campaign_market,
                   definitions.campaign_definition -> 'execution_scenarios'
                       AS campaign_scenarios
        ) AS campaign_policy
        CROSS JOIN LATERAL (
            SELECT campaign_policy.campaign_entry || jsonb_build_object(
                       'candidate_timeframe_seconds',
                           definitions.candidate_definition -> 'timeframe_seconds',
                       'schema', 'systematic_fx.bar_entry_policy.v1'
                   ) AS entry_policy,
                   campaign_policy.campaign_barriers || jsonb_build_object(
                       'grid_evaluation', 'ALL_CELLS_NO_EARLY_PRUNING',
                       'ticks_per_pip', campaign_policy.campaign_market -> 'ticks_per_pip',
                       'schema', 'systematic_fx.bar_barrier_policy.v1'
                   ) AS barrier_policy,
                   jsonb_build_object(
                       'holding_limit_policy',
                           campaign_policy.campaign_entry -> 'holding_limit_policy',
                       'normal_market_closure_policy',
                           campaign_policy.campaign_entry -> 'normal_market_closure_policy',
                       'outcome_span_policy_sha256',
                           '1a8948a7675d9da770c083b7bf07fdd1f755a202796c69df1a5d57cfece966b9',
                       'split_boundary_policy', 'TERMINAL_EXIT_AT_DISCOVERY_END',
                       'terminal_boundary_types',
                           campaign_policy.campaign_entry -> 'terminal_boundary_types',
                       'schema', 'systematic_fx.bar_terminal_policy.v1'
                   ) AS terminal_policy
        ) AS base_policy
        CROSS JOIN LATERAL (
            SELECT jsonb_build_object(
                       'bars', campaign_policy.campaign_bars,
                       'candidate_definition', definitions.candidate_definition,
                       'candidate_definition_sha256',
                           trial.parameters -> 'candidate_definition_sha256',
                       'candidate_key', to_jsonb(trial.trial_key),
                       'decision_visibility', 'DISCOVERY_DECISION_DATES_ONLY',
                       'schema', 'systematic_fx.bar_signal_policy.v1'
                   ) AS signal_policy,
                   jsonb_build_object(
                       'base_monthly_fixed_pool_usd', '500.00',
                       'execution_scenarios', campaign_policy.campaign_scenarios,
                       'expected_monthly_round_trips', 20,
                       'fixed_cost_allocation',
                           'MONTHLY_POOL_DIVIDED_BY_ROUND_TRIPS_CEILING_TICKS',
                       'market', campaign_policy.campaign_market,
                       'schema', 'systematic_fx.bar_cost_policy.v1',
                       'tick_value_usd', '6.25'
                   ) AS cost_policy,
                   jsonb_build_object(
                       'candidate_budget',
                           definitions.campaign_definition -> 'candidate_budget',
                       'discovery_economic_gates',
                           definitions.campaign_definition -> 'discovery_economic_gates',
                       'discovery_support_gates',
                           definitions.campaign_definition -> 'discovery_support_gates',
                       'finalist_limit',
                           definitions.campaign_definition #> '{holdout_gates,maximum_finalists}',
                       'ranking_order', jsonb_build_array(
                           'positive_block_count_desc',
                           'worst_block_moderate_ev_desc',
                           'overall_moderate_ev_desc',
                           'moderate_maximum_drawdown_asc',
                           'selected_stop_loss_ticks_asc',
                           'selected_take_profit_ticks_asc',
                           'candidate_key_asc'
                       ),
                       'schema', 'systematic_fx.bar_selection_policy.v1'
                   ) AS selection_policy,
                   jsonb_build_object(
                       'candidate_summary', 'FULL_3_SCENARIO_484_CELL_SURFACES',
                       'evidence_schema',
                           'systematic_fx.bar_pattern_discovery_evidence.v1',
                       'match_shard_max_records', 4096,
                       'publication', 'CONTENT_ADDRESSED_HELD_DIRFD',
                       'record_kinds', jsonb_build_array('matches', 'replays'),
                       'replay_shard_max_records', 256,
                       'spool_version', 'bar_pattern_discovery_spool_v1',
                       'schema', 'systematic_fx.bar_evidence_policy.v1'
                   ) AS evidence_policy
        ) AS policy
        CROSS JOIN LATERAL (
            SELECT jsonb_build_object(
                       'barrier_policy', base_policy.barrier_policy,
                       'entry_policy', base_policy.entry_policy,
                       'one_position_policy',
                           'INDEPENDENT_PER_CANDIDATE_SCENARIO_TP_SL_CELL',
                       'same_second_first_touch_policy',
                           campaign_policy.campaign_entry -> 'same_second_first_touch_policy',
                       'terminal_policy', base_policy.terminal_policy,
                       'schema', 'systematic_fx.bar_execution_policy.v1'
                   ) AS execution_policy,
                   jsonb_build_object(
                       'barrier_policy', base_policy.barrier_policy,
                       'one_second_path_source',
                           'VERIFIED_SELECTED_CONTRACT_TRADE_BARS',
                       'outcome_span_policy_sha256',
                           '1a8948a7675d9da770c083b7bf07fdd1f755a202796c69df1a5d57cfece966b9',
                       'same_second_first_touch_policy',
                           campaign_policy.campaign_entry -> 'same_second_first_touch_policy',
                       'terminal_policy', base_policy.terminal_policy,
                       'schema', 'systematic_fx.bar_outcome_policy.v1'
                   ) AS outcome_policy
        ) AS compound_policy
        CROSS JOIN LATERAL (
            SELECT jsonb_build_object(
                       'bar_feature_version', 'selected_contract_trade_ohlcv_bars_v1',
                       'candidate_catalog_sha256',
                           trial.parameters -> 'candidate_catalog_sha256'
                   ) AS feature_versions,
                   jsonb_build_object(
                       'allocated_candidate_count', 216,
                       'bar_dataset_manifest_sha256',
                           trial.parameters -> 'bar_dataset_manifest_sha256',
                       'campaign_definition_sha256',
                           trial.parameters -> 'campaign_definition_sha256',
                       'raw_source_manifest_sha256',
                           trial.parameters -> 'raw_source_manifest_sha256',
                       'result_driven_additions_allowed', false,
                       'split_plan_sha256', trial.parameters -> 'split_plan_sha256',
                       'unallocated_campaign_budget', 24
                   ) AS search_boundary,
                   jsonb_build_object(
                       'execution_scenarios', campaign_policy.campaign_scenarios
                   ) AS cost_assumptions,
                   campaign_policy.campaign_entry AS execution_assumptions
        ) AS registry_policy
        CROSS JOIN LATERAL (
            SELECT jsonb_build_object(
                       'cost_assumptions', registry_policy.cost_assumptions,
                       'execution_assumptions', registry_policy.execution_assumptions,
                       'feature_versions', registry_policy.feature_versions,
                       'search_boundary', registry_policy.search_boundary
                   ) AS experiment_config
        ) AS registry_identity
        WHERE trial.experiment_trial_id = target_trial_id
          AND experiment.experiment_key =
              'bar_pattern_discovery_v1:experiment:frozen_candidate_catalog:v1'
          AND campaign.campaign_key = 'bar_pattern_discovery_v1'
          AND trial.experiment_id = experiment.experiment_id
          AND trial.trial_type = 'STRATEGY_VARIANT'
          AND systematic_fx.jsonb_has_exact_keys(
              trial.parameters,
              ARRAY[
                  'bar_dataset_manifest_sha256', 'campaign_definition',
                  'campaign_definition_sha256', 'candidate_catalog_sha256',
                  'candidate_definition', 'candidate_definition_sha256',
                  'candidate_key', 'raw_source_manifest_sha256', 'schema',
                  'split_plan', 'split_plan_schema', 'split_plan_sha256'
              ]
          )
          AND trial.parameters #>> '{schema}' =
              'systematic_fx.bar_pattern_trial_parameters.v1'
          AND trial.parameters #>> '{candidate_key}' = trial.trial_key
          AND trial.parameters #>> '{candidate_definition,candidate_key}' = trial.trial_key
          AND trial.parameters #>> '{candidate_definition,direction}' IN ('LONG', 'SHORT')
          AND trial.parameters #>> '{split_plan_schema}' =
              'systematic_fx.bar_pattern_splits.v1'
          AND trial.parameters_sha256 =
              systematic_fx.canonical_jsonb_sha256(trial.parameters)
          AND trial.parameters #>> '{campaign_definition_sha256}' =
              systematic_fx.canonical_jsonb_sha256(definitions.campaign_definition)
          AND trial.parameters #>> '{campaign_definition_sha256}' =
              '8515c02921da6a1da31edb49a4809e048ae931a4ae32741559216eac1cc74081'
          AND trial.parameters #>> '{candidate_definition_sha256}' =
              systematic_fx.canonical_jsonb_sha256(definitions.candidate_definition)
          AND trial.parameters #>> '{split_plan_sha256}' =
              systematic_fx.canonical_jsonb_sha256(trial.parameters -> 'split_plan')
          AND trial.parameters #>> '{split_plan_sha256}' =
              '5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043'
          AND trial.parameters #>> '{campaign_definition,campaign_key}' =
              campaign.campaign_key
          AND trial.parameters #>> '{campaign_definition,candidate_catalog_sha256}' =
              trial.parameters #>> '{candidate_catalog_sha256}'
          AND trial.parameters #>> '{candidate_catalog_sha256}' =
              '8be2d000af8c8ce3a41e3128ca3c503831e39d36e8590b8e04780632d1cc7562'
          AND trial.parameters #>> '{campaign_definition,config_semantic_sha256}' =
              '34b84587e12af32f84bdcc3e66552c763feccbc55043d8514e188fb8895c7283'
          AND trial.parameters #>> '{campaign_definition,qualification_status}' =
              'BLOCKED_MISSING_POINT_IN_TIME_DEFINITION_AND_TRADING_STATUS'
          AND trial.parameters #> '{campaign_definition,screening_only}' = 'true'::jsonb
          AND trial.parameters #>> '{raw_source_manifest_sha256}' =
              '14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de'
          AND dataset.dataset_key = 'glbx_mdp3_mbp_10_6e_fut_v1'
          AND trial.parameters #>> '{bar_dataset_manifest_sha256}' =
              'e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc'
          AND dataset.manifest_sha256 = trial.parameters #>> '{bar_dataset_manifest_sha256}'
          AND dataset.status NOT IN ('REJECTED', 'RETIRED')
          AND campaign.name = 'Frozen multi-timeframe OHLC bar-pattern screening'
          AND campaign.status = 'FROZEN'
          AND campaign.frozen_at IS NOT NULL
          AND campaign.holdout_revealed_at IS NULL
          AND campaign.closed_at IS NULL
          AND campaign.selected_start_date =
              (trial.parameters #>> '{split_plan,eligible_start_date}')::date
          AND campaign.selected_end_date =
              (trial.parameters #>> '{split_plan,eligible_end_date}')::date
          AND campaign.roll_cutoff_date IS NULL
          AND campaign.config_sha256 = trial.parameters #>> '{campaign_definition_sha256}'
          AND campaign.data_manifest_sha256 =
              trial.parameters #>> '{bar_dataset_manifest_sha256}'
          AND campaign.split_policy = jsonb_build_object(
              'bar_dataset_manifest_sha256',
                  trial.parameters #>> '{bar_dataset_manifest_sha256}',
              'raw_source_manifest_sha256',
                  trial.parameters #>> '{raw_source_manifest_sha256}',
              'split_plan', trial.parameters -> 'split_plan',
              'split_plan_sha256', trial.parameters #>> '{split_plan_sha256}'
          )
          AND campaign.feature_version = 'selected_contract_trade_ohlcv_bars_v1'
          AND campaign.outcome_version = 'bar_first_touch_surface_v1'
          AND campaign.cost_model_version = 'bar_conservative_combined_cost_v1'
          AND campaign.execution_model_version = 'bar_next_open_stop_first_v1'
          AND campaign.trial_budget = 240
          AND campaign.finalist_budget = 10
          AND experiment.campaign_id = campaign.campaign_id
          AND experiment.pattern_id IS NULL
          AND experiment.parent_experiment_id IS NULL
          AND experiment.primary_family = 'FIXED_OHLC_BAR_PATTERN_CATALOG'
          AND experiment.status = 'FROZEN'
          AND experiment.hypothesis =
              'Fixed OHLC setup and trigger patterns have stable next-open first-touch economics'
          AND experiment.direction = 'BOTH'
          AND experiment.model_family = 'RULE_BASED_FIXED_OHLC'
          AND experiment.tick_size = 0.00005
          AND experiment.tick_value = 6.25
          AND experiment.feature_definition_versions = registry_policy.feature_versions
          AND experiment.search_boundary = registry_policy.search_boundary
          AND experiment.cost_assumptions = registry_policy.cost_assumptions
          AND experiment.execution_assumptions = registry_policy.execution_assumptions
          AND experiment.trial_budget = 216
          AND experiment.trials_registered = 216
          AND experiment.code_commit = campaign.code_commit
          AND experiment.config_sha256 =
              systematic_fx.canonical_jsonb_sha256(registry_identity.experiment_config)
          AND experiment.registration_artifact_id = registration_artifact.artifact_id
          AND experiment.frozen_at IS NOT NULL
          AND experiment.completed_at IS NULL
          AND registration_artifact.artifact_type = 'bar_registration'
          AND registration_artifact.media_type = 'application/json'
          AND registration_artifact.sha256 =
              registration_artifact.metadata #>> '{content_sha256}'
          AND registration_artifact.artifact_key =
              registration_artifact.metadata #>> '{artifact_key}'
          AND registration_artifact.metadata #>> '{artifact_schema}' =
              'systematic_fx.bar_pattern_registration.v1'
          AND registration_artifact.metadata #>> '{artifact_type}' = 'bar_registration'
          AND registration_artifact.metadata #> '{artifact_version}' = '1'::jsonb
          AND registration_artifact.metadata #> '{record_count}' = '216'::jsonb
          AND registration_artifact.metadata #>> '{schema_sha256}' =
              '940da1af8646df11f04db5a1c67883187833bd472507b1b93f3656d3011e1df3'
          AND registration_artifact.metadata #>> '{source_manifest_sha256}' =
              trial.parameters #>> '{bar_dataset_manifest_sha256}'
          AND registration_artifact.metadata #>> '{root_kind}' = 'bar_patterns'
          AND registration_artifact.metadata #>> '{file_suffix}' = '.json'
          AND registration_artifact.metadata #>> '{identity_schema}' =
              'systematic_fx.bar_artifact_identity.v1'
          AND registration_artifact.metadata #>> '{logical_identity,bar_dataset_manifest_sha256}' =
              trial.parameters #>> '{bar_dataset_manifest_sha256}'
          AND registration_artifact.metadata #>> '{logical_identity,campaign_definition_sha256}' =
              trial.parameters #>> '{campaign_definition_sha256}'
          AND registration_artifact.metadata #>> '{logical_identity,candidate_catalog_sha256}' =
              trial.parameters #>> '{candidate_catalog_sha256}'
          AND registration_artifact.metadata #>> '{logical_identity,code_commit}' =
              campaign.code_commit
          AND registration_artifact.metadata #>> '{logical_identity,raw_source_manifest_sha256}' =
              trial.parameters #>> '{raw_source_manifest_sha256}'
          AND registration_artifact.metadata #>> '{logical_identity,split_plan_sha256}' =
              trial.parameters #>> '{split_plan_sha256}'
          AND registration_artifact.metadata #>> '{logical_identity,document_sha256}' =
              registration_artifact.sha256
          AND registration_artifact.metadata #>> '{artifact_identity_sha256}' =
              systematic_fx.canonical_jsonb_sha256(
                  registration_artifact.metadata
                      - 'artifact_identity_sha256' - 'content_sha256'
              )
          AND registration_artifact.artifact_key =
              'bar_pattern_discovery_v1:registration:' ||
              (registration_artifact.metadata #>> '{logical_identity,document_sha256}')
          AND (
              SELECT count(*)
              FROM systematic_fx.experiment_trials AS catalog_trial
              WHERE catalog_trial.experiment_id = experiment.experiment_id
          ) = 216
          AND (
              SELECT systematic_fx.canonical_jsonb_sha256(
                  jsonb_agg(
                      catalog_trial.parameters -> 'candidate_definition'
                      ORDER BY catalog_trial.trial_key COLLATE "C"
                  )
              )
              FROM systematic_fx.experiment_trials AS catalog_trial
              WHERE catalog_trial.experiment_id = experiment.experiment_id
          ) = trial.parameters #>> '{candidate_catalog_sha256}'
          AND NOT EXISTS (
              SELECT 1
              FROM systematic_fx.experiment_trials AS catalog_trial
              WHERE catalog_trial.experiment_id = experiment.experiment_id
                AND (
                    catalog_trial.trial_type IS DISTINCT FROM 'STRATEGY_VARIANT'
                    OR NOT systematic_fx.jsonb_has_exact_keys(
                        catalog_trial.parameters,
                        ARRAY[
                            'bar_dataset_manifest_sha256', 'campaign_definition',
                            'campaign_definition_sha256', 'candidate_catalog_sha256',
                            'candidate_definition', 'candidate_definition_sha256',
                            'candidate_key', 'raw_source_manifest_sha256', 'schema',
                            'split_plan', 'split_plan_schema', 'split_plan_sha256'
                        ]
                    )
                    OR catalog_trial.parameters #>> '{schema}' IS DISTINCT FROM
                       'systematic_fx.bar_pattern_trial_parameters.v1'
                    OR catalog_trial.parameters #>> '{candidate_key}'
                       IS DISTINCT FROM catalog_trial.trial_key
                    OR catalog_trial.parameters #>> '{candidate_definition,candidate_key}'
                       IS DISTINCT FROM catalog_trial.trial_key
                    OR catalog_trial.parameters_sha256 IS DISTINCT FROM
                       systematic_fx.canonical_jsonb_sha256(catalog_trial.parameters)
                    OR catalog_trial.parameters #>> '{candidate_definition_sha256}'
                       IS DISTINCT FROM systematic_fx.canonical_jsonb_sha256(
                           catalog_trial.parameters -> 'candidate_definition'
                       )
                    OR catalog_trial.parameters #>> '{campaign_definition_sha256}'
                       IS DISTINCT FROM trial.parameters #>> '{campaign_definition_sha256}'
                    OR catalog_trial.parameters #>> '{candidate_catalog_sha256}'
                       IS DISTINCT FROM trial.parameters #>> '{candidate_catalog_sha256}'
                    OR catalog_trial.parameters #>> '{raw_source_manifest_sha256}'
                       IS DISTINCT FROM trial.parameters #>> '{raw_source_manifest_sha256}'
                    OR catalog_trial.parameters #>> '{bar_dataset_manifest_sha256}'
                       IS DISTINCT FROM trial.parameters #>> '{bar_dataset_manifest_sha256}'
                    OR catalog_trial.parameters #>> '{split_plan_sha256}'
                       IS DISTINCT FROM trial.parameters #>> '{split_plan_sha256}'
                )
          )
          AND run_spec.campaign_id = experiment.campaign_id
          AND run_spec.experiment_id = experiment.experiment_id
          AND run_spec.parent_run_spec_id IS NULL
          AND run_spec.run_kind = 'SCREEN'
          AND run_spec.canonicalization_schema = 'systematic_fx.research_run_spec.v2'
          AND run_spec.canonicalization_version = 2
          AND run_spec.engine_version = 'bar_pattern_streaming_discovery_v1'
          AND run_spec.eligible_calendar_version = 'bar_dataset_eligible_calendar_v1'
          AND run_spec.eligible_calendar_sha256 =
              '92e2112c24463f3a9a2f59182a4ad6099e6a8fc740f3d8ddc771d31e61c1163d'
          AND run_spec.split_version = 'bar_pattern_splits_v1'
          AND run_spec.split_sha256 =
              '5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043'
          AND run_spec.feature_version = 'selected_contract_trade_ohlcv_bars_v1'
          AND run_spec.outcome_version = 'bar_first_touch_surface_v1'
          AND run_spec.cost_version = 'bar_conservative_combined_cost_v1'
          AND run_spec.execution_version = 'bar_next_open_stop_first_v1'
          AND run_spec.deterministic_seed = 0
          AND run_spec.direction = trial.parameters #>> '{candidate_definition,direction}'
          AND run_spec.code_commit = campaign.code_commit
          AND run_spec.run_fingerprint =
              systematic_fx.canonical_jsonb_sha256(run_spec.canonical_spec)
          AND run_spec.source_manifest_hashes = jsonb_build_object(
              'raw_mbp10_source_manifest_v1',
                  trial.parameters #>> '{raw_source_manifest_sha256}',
              'selected_trade_bar_dataset_manifest_v1',
                  trial.parameters #>> '{bar_dataset_manifest_sha256}'
          )
          AND systematic_fx.jsonb_has_exact_keys(
              run_spec.canonical_spec,
              ARRAY[
                  'artifact_schema', 'barrier_policy', 'campaign_id',
                  'code_commit', 'code_snapshot_sha256', 'cost',
                  'dependency_lock_sha256', 'direction', 'eligible_calendar',
                  'engine_version', 'entry_policy', 'execution', 'experiment_id',
                  'feature', 'outcome', 'parameters', 'random_seed', 'run_kind',
                  'runtime_environment', 'schema_version', 'signal_policy',
                  'source_manifest_hashes', 'split', 'terminal_policy'
              ]
          )
          AND run_spec.canonical_spec #>> '{artifact_schema}' =
              'systematic_fx.research_run_spec.v2'
          AND run_spec.canonical_spec #> '{schema_version}' = '2'::jsonb
          AND run_spec.canonical_spec #>> '{campaign_id}' = campaign.campaign_key
          AND run_spec.canonical_spec #>> '{experiment_id}' = experiment.experiment_key
          AND run_spec.canonical_spec #>> '{run_kind}' = run_spec.run_kind
          AND run_spec.canonical_spec #>> '{engine_version}' = run_spec.engine_version
          AND run_spec.canonical_spec #>> '{code_commit}' = run_spec.code_commit
          AND run_spec.canonical_spec #>> '{code_snapshot_sha256}' =
              run_spec.code_snapshot_sha256
          AND run_spec.canonical_spec #>> '{dependency_lock_sha256}' =
              run_spec.dependency_lock_sha256
          AND run_spec.canonical_spec #> '{random_seed}' = '0'::jsonb
          AND run_spec.canonical_spec #>> '{direction}' = run_spec.direction
          AND run_spec.canonical_spec -> 'source_manifest_hashes' =
              run_spec.source_manifest_hashes
          AND run_spec.canonical_spec -> 'eligible_calendar' = jsonb_build_object(
              'version', run_spec.eligible_calendar_version,
              'sha256', run_spec.eligible_calendar_sha256
          )
          AND run_spec.canonical_spec -> 'split' = jsonb_build_object(
              'version', run_spec.split_version, 'sha256', run_spec.split_sha256
          )
          AND run_spec.canonical_spec -> 'feature' = jsonb_build_object(
              'version', run_spec.feature_version, 'sha256', run_spec.feature_sha256
          )
          AND run_spec.canonical_spec -> 'outcome' = jsonb_build_object(
              'version', run_spec.outcome_version, 'sha256', run_spec.outcome_sha256
          )
          AND run_spec.canonical_spec -> 'cost' = jsonb_build_object(
              'version', run_spec.cost_version, 'sha256', run_spec.cost_sha256
          )
          AND run_spec.canonical_spec -> 'execution' = jsonb_build_object(
              'version', run_spec.execution_version, 'sha256', run_spec.execution_sha256
          )
          AND run_spec.split_sha256 = trial.parameters #>> '{split_plan_sha256}'
          AND run_spec.feature_sha256 =
              systematic_fx.canonical_jsonb_sha256(policy.signal_policy)
          AND run_spec.outcome_sha256 =
              systematic_fx.canonical_jsonb_sha256(compound_policy.outcome_policy)
          AND run_spec.cost_sha256 =
              systematic_fx.canonical_jsonb_sha256(policy.cost_policy)
          AND run_spec.execution_sha256 =
              systematic_fx.canonical_jsonb_sha256(compound_policy.execution_policy)
          AND run_spec.canonical_spec -> 'signal_policy' = policy.signal_policy
          AND run_spec.canonical_spec -> 'entry_policy' = base_policy.entry_policy
          AND run_spec.canonical_spec -> 'barrier_policy' = base_policy.barrier_policy
          AND run_spec.canonical_spec -> 'terminal_policy' = base_policy.terminal_policy
          AND systematic_fx.jsonb_has_exact_keys(
              run_spec.canonical_spec -> 'parameters',
              ARRAY[
                  'bar_barrier_policy_sha256', 'bar_campaign_definition_sha256',
                  'bar_candidate_catalog_sha256',
                  'bar_candidate_definition_sha256', 'bar_candidate_key',
                  'bar_code_snapshot_artifact_identity_sha256',
                  'bar_config_file_sha256', 'bar_config_semantic_sha256',
                  'bar_cost_policy', 'bar_cost_policy_sha256',
                  'bar_dataset_handoff_sha256', 'bar_dataset_manifest_sha256',
                  'bar_entry_policy_sha256', 'bar_evidence_policy',
                  'bar_evidence_policy_sha256', 'bar_execution_policy',
                  'bar_outcome_policy', 'bar_outcome_span_policy_sha256',
                  'bar_postgres_migrations_sha256', 'bar_raw_source_manifest_sha256',
                  'bar_screening_only', 'bar_selection_policy',
                  'bar_selection_policy_sha256', 'bar_split_plan_sha256',
                  'bar_trial_parameters_sha256', 'qualification_status'
              ]
          )
          AND run_spec.canonical_spec #>> '{parameters,bar_candidate_key}' = trial.trial_key
          AND run_spec.canonical_spec #>> '{parameters,bar_trial_parameters_sha256}' =
              trial.parameters_sha256
          AND run_spec.canonical_spec #>> '{parameters,bar_campaign_definition_sha256}' =
              trial.parameters #>> '{campaign_definition_sha256}'
          AND run_spec.canonical_spec #>> '{parameters,bar_candidate_catalog_sha256}' =
              trial.parameters #>> '{candidate_catalog_sha256}'
          AND run_spec.canonical_spec #>> '{parameters,bar_candidate_definition_sha256}' =
              trial.parameters #>> '{candidate_definition_sha256}'
          AND run_spec.canonical_spec #>> '{parameters,bar_config_semantic_sha256}' =
              trial.parameters #>> '{campaign_definition,config_semantic_sha256}'
          AND run_spec.canonical_spec #>> '{parameters,bar_config_file_sha256}' =
              '708d2127423cb1f9b61c5ca76c8d95cdf6b32ca5c45cd2606c1870017ae2b102'
          AND run_spec.canonical_spec #>> '{parameters,bar_dataset_handoff_sha256}' =
              '26b1bb96f7323cae13bbe5d670c12f3e85615bbb9aab56932ce6523e67af7b00'
          AND run_spec.canonical_spec #>> '{parameters,bar_dataset_manifest_sha256}' =
              trial.parameters #>> '{bar_dataset_manifest_sha256}'
          AND run_spec.canonical_spec #>> '{parameters,bar_raw_source_manifest_sha256}' =
              trial.parameters #>> '{raw_source_manifest_sha256}'
          AND run_spec.canonical_spec #>> '{parameters,bar_split_plan_sha256}' =
              trial.parameters #>> '{split_plan_sha256}'
          AND run_spec.canonical_spec #>> '{parameters,bar_outcome_span_policy_sha256}' =
              '1a8948a7675d9da770c083b7bf07fdd1f755a202796c69df1a5d57cfece966b9'
          AND run_spec.canonical_spec #> '{parameters,bar_screening_only}' = 'true'::jsonb
          AND run_spec.canonical_spec #>> '{parameters,qualification_status}' =
              'BLOCKED_MISSING_POINT_IN_TIME_DEFINITION_AND_TRADING_STATUS'
          AND run_spec.canonical_spec #>> '{parameters,bar_barrier_policy_sha256}' =
              systematic_fx.canonical_jsonb_sha256(base_policy.barrier_policy)
          AND run_spec.canonical_spec #>> '{parameters,bar_entry_policy_sha256}' =
              systematic_fx.canonical_jsonb_sha256(base_policy.entry_policy)
          AND run_spec.canonical_spec #>> '{parameters,bar_cost_policy_sha256}' =
              systematic_fx.canonical_jsonb_sha256(policy.cost_policy)
          AND run_spec.canonical_spec #>> '{parameters,bar_evidence_policy_sha256}' =
              systematic_fx.canonical_jsonb_sha256(policy.evidence_policy)
          AND run_spec.canonical_spec #>> '{parameters,bar_selection_policy_sha256}' =
              systematic_fx.canonical_jsonb_sha256(policy.selection_policy)
          AND run_spec.canonical_spec #> '{parameters,bar_cost_policy}' =
              policy.cost_policy
          AND run_spec.canonical_spec #> '{parameters,bar_evidence_policy}' =
              policy.evidence_policy
          AND run_spec.canonical_spec #> '{parameters,bar_selection_policy}' =
              policy.selection_policy
          AND run_spec.canonical_spec #> '{parameters,bar_execution_policy}' =
              compound_policy.execution_policy
          AND run_spec.canonical_spec #> '{parameters,bar_outcome_policy}' =
              compound_policy.outcome_policy
          AND systematic_fx.jsonb_has_exact_keys(
              run_spec.canonical_spec -> 'runtime_environment',
              ARRAY[
                  'artifact_schema', 'bar_research_run', 'cpu_count', 'locale',
                  'numeric_environment', 'packages', 'platform', 'postgresql',
                  'python', 'timezone'
              ]
          )
          AND run_spec.canonical_spec #>> '{runtime_environment,artifact_schema}' =
              'systematic_fx.runtime_environment.v1'
          AND run_spec.canonical_spec #>>
              '{runtime_environment,bar_research_run,engine_version}' =
              run_spec.engine_version
          AND run_spec.canonical_spec #>>
              '{runtime_environment,bar_research_run,orchestration}' =
              'REGISTER_AND_START_ALL_BEFORE_SINGLE_DISCOVERY_PASS'
          AND run_spec.canonical_spec #>>
              '{runtime_environment,bar_research_run,dataset_handoff_sha256}' =
              run_spec.canonical_spec #>> '{parameters,bar_dataset_handoff_sha256}'
          AND run_spec.canonical_spec #>>
              '{runtime_environment,bar_research_run,code_snapshot_artifact_identity_sha256}' =
              run_spec.canonical_spec #>>
                  '{parameters,bar_code_snapshot_artifact_identity_sha256}'
          AND run_spec.canonical_spec #>>
              '{runtime_environment,postgresql,schema_migrations_sha256}' =
              run_spec.canonical_spec #>> '{parameters,bar_postgres_migrations_sha256}'
          AND EXISTS (
              SELECT 1
              FROM systematic_fx.artifacts AS snapshot
              WHERE snapshot.artifact_type = 'bar_code_snapshot'
                AND snapshot.sha256 = run_spec.code_snapshot_sha256
                AND snapshot.metadata #>> '{artifact_identity_sha256}' =
                    run_spec.canonical_spec #>>
                        '{parameters,bar_code_snapshot_artifact_identity_sha256}'
                AND snapshot.metadata #>> '{logical_identity,code_commit}' =
                    run_spec.code_commit
                AND snapshot.metadata #>> '{logical_identity,dataset_handoff_sha256}' =
                    run_spec.canonical_spec #>> '{parameters,bar_dataset_handoff_sha256}'
                AND snapshot.metadata #>> '{logical_identity,dataset_manifest_sha256}' =
                    trial.parameters #>> '{bar_dataset_manifest_sha256}'
                AND snapshot.metadata #>> '{logical_identity,outcome_span_policy_sha256}' =
                    '1a8948a7675d9da770c083b7bf07fdd1f755a202796c69df1a5d57cfece966b9'
                AND snapshot.metadata #>> '{logical_identity,raw_source_manifest_sha256}' =
                    trial.parameters #>> '{raw_source_manifest_sha256}'
          )
    )
    SELECT EXISTS (SELECT 1 FROM bound);
$$;

CREATE FUNCTION systematic_fx.protect_bar_pattern_campaign_identity()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF OLD.campaign_key IS DISTINCT FROM 'bar_pattern_discovery_v1' THEN
        IF TG_OP = 'UPDATE'
           AND NEW.campaign_key IS NOT DISTINCT FROM 'bar_pattern_discovery_v1' THEN
            RAISE EXCEPTION 'campaigns cannot be mutated into bar-pattern governance';
        END IF;
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'the governed bar-pattern campaign is append-preserved';
    END IF;
    IF NEW.campaign_key IS DISTINCT FROM OLD.campaign_key
       OR NEW.dataset_id IS DISTINCT FROM OLD.dataset_id
       OR NEW.name IS DISTINCT FROM OLD.name
       OR NEW.status IS DISTINCT FROM OLD.status
       OR NEW.selected_start_date IS DISTINCT FROM OLD.selected_start_date
       OR NEW.selected_end_date IS DISTINCT FROM OLD.selected_end_date
       OR NEW.roll_cutoff_date IS DISTINCT FROM OLD.roll_cutoff_date
       OR NEW.data_manifest_sha256 IS DISTINCT FROM OLD.data_manifest_sha256
       OR NEW.feature_version IS DISTINCT FROM OLD.feature_version
       OR NEW.outcome_version IS DISTINCT FROM OLD.outcome_version
       OR NEW.cost_model_version IS DISTINCT FROM OLD.cost_model_version
       OR NEW.execution_model_version IS DISTINCT FROM OLD.execution_model_version
       OR NEW.code_commit IS DISTINCT FROM OLD.code_commit
       OR NEW.config_sha256 IS DISTINCT FROM OLD.config_sha256
       OR NEW.split_policy IS DISTINCT FROM OLD.split_policy
       OR NEW.trial_budget IS DISTINCT FROM OLD.trial_budget
       OR NEW.finalist_budget IS DISTINCT FROM OLD.finalist_budget
       OR NEW.created_at IS DISTINCT FROM OLD.created_at
       OR NEW.frozen_at IS DISTINCT FROM OLD.frozen_at
       OR NEW.holdout_revealed_at IS DISTINCT FROM OLD.holdout_revealed_at
       OR NEW.closed_at IS DISTINCT FROM OLD.closed_at THEN
        RAISE EXCEPTION 'the governed bar-pattern campaign identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER campaigns_protect_bar_pattern_identity
BEFORE UPDATE OR DELETE ON systematic_fx.campaigns
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_bar_pattern_campaign_identity();

CREATE FUNCTION systematic_fx.protect_bar_pattern_experiment_identity()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF OLD.experiment_key IS DISTINCT FROM
       'bar_pattern_discovery_v1:experiment:frozen_candidate_catalog:v1' THEN
        IF TG_OP = 'UPDATE'
           AND NEW.experiment_key IS NOT DISTINCT FROM
               'bar_pattern_discovery_v1:experiment:frozen_candidate_catalog:v1' THEN
            RAISE EXCEPTION 'experiments cannot be mutated into bar-pattern governance';
        END IF;
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'the governed bar-pattern experiment is append-preserved';
    END IF;
    IF NEW.experiment_key IS DISTINCT FROM OLD.experiment_key
       OR NEW.campaign_id IS DISTINCT FROM OLD.campaign_id
       OR NEW.pattern_id IS DISTINCT FROM OLD.pattern_id
       OR NEW.parent_experiment_id IS DISTINCT FROM OLD.parent_experiment_id
       OR NEW.primary_family IS DISTINCT FROM OLD.primary_family
       OR NEW.status IS DISTINCT FROM OLD.status
       OR NEW.hypothesis IS DISTINCT FROM OLD.hypothesis
       OR NEW.direction IS DISTINCT FROM OLD.direction
       OR NEW.model_family IS DISTINCT FROM OLD.model_family
       OR NEW.tick_size IS DISTINCT FROM OLD.tick_size
       OR NEW.tick_value IS DISTINCT FROM OLD.tick_value
       OR NEW.feature_definition_versions IS DISTINCT FROM OLD.feature_definition_versions
       OR NEW.search_boundary IS DISTINCT FROM OLD.search_boundary
       OR NEW.cost_assumptions IS DISTINCT FROM OLD.cost_assumptions
       OR NEW.execution_assumptions IS DISTINCT FROM OLD.execution_assumptions
       OR NEW.trial_budget IS DISTINCT FROM OLD.trial_budget
       OR NEW.trials_registered IS DISTINCT FROM OLD.trials_registered
       OR NEW.registration_artifact_id IS DISTINCT FROM OLD.registration_artifact_id
       OR NEW.code_commit IS DISTINCT FROM OLD.code_commit
       OR NEW.config_sha256 IS DISTINCT FROM OLD.config_sha256
       OR NEW.registered_at IS DISTINCT FROM OLD.registered_at
       OR NEW.frozen_at IS DISTINCT FROM OLD.frozen_at
       OR NEW.completed_at IS DISTINCT FROM OLD.completed_at THEN
        RAISE EXCEPTION 'the governed bar-pattern experiment identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER experiments_protect_bar_pattern_identity
BEFORE UPDATE OR DELETE ON systematic_fx.experiments
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_bar_pattern_experiment_identity();

CREATE FUNCTION systematic_fx.enforce_bar_pattern_trial_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    existing_trial_count integer;
BEGIN
    IF NOT systematic_fx.bar_pattern_experiment_is_governed(NEW.experiment_id) THEN
        RETURN NEW;
    END IF;
    SELECT count(*)::integer
    INTO existing_trial_count
    FROM systematic_fx.experiment_trials AS existing
    WHERE existing.experiment_id = NEW.experiment_id;
    IF existing_trial_count >= 216 THEN
        RAISE EXCEPTION 'the governed bar-pattern catalog cannot exceed 216 trials';
    END IF;
    IF NEW.trial_type IS DISTINCT FROM 'STRATEGY_VARIANT'
       OR NEW.status IS DISTINCT FROM 'REGISTERED'
       OR NEW.research_run_spec_id IS NOT NULL
       OR NEW.result_summary IS DISTINCT FROM '{}'::jsonb
       OR NEW.started_at IS NOT NULL
       OR NEW.finished_at IS NOT NULL THEN
        RAISE EXCEPTION
            'governed bar-pattern trials must be inserted as pristine unbound registrations';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER experiment_trials_enforce_bar_pattern_insert
BEFORE INSERT ON systematic_fx.experiment_trials
FOR EACH ROW EXECUTE FUNCTION systematic_fx.enforce_bar_pattern_trial_insert();

CREATE FUNCTION systematic_fx.bar_pattern_catalog_attempts_preregistered(
    target_spec_id bigint
)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM systematic_fx.research_run_specs AS target
        JOIN systematic_fx.experiments AS experiment
          ON experiment.experiment_id = target.experiment_id
        WHERE target.research_run_spec_id = target_spec_id
          AND experiment.experiment_key =
              'bar_pattern_discovery_v1:experiment:frozen_candidate_catalog:v1'
          AND (
              SELECT count(*)
              FROM systematic_fx.experiment_trials AS catalog_trial
              WHERE catalog_trial.experiment_id = experiment.experiment_id
          ) = 216
          AND NOT EXISTS (
              SELECT 1
              FROM systematic_fx.experiment_trials AS catalog_trial
              WHERE catalog_trial.experiment_id = experiment.experiment_id
                AND (
                    catalog_trial.research_run_spec_id IS NULL
                    OR NOT EXISTS (
                        SELECT 1
                        FROM systematic_fx.research_run_attempts AS catalog_attempt
                        WHERE catalog_attempt.research_run_spec_id =
                              catalog_trial.research_run_spec_id
                          AND catalog_attempt.status IN ('RUNNING', 'SUCCEEDED')
                    )
                )
          )
    );
$$;

CREATE FUNCTION systematic_fx.protect_bar_pattern_trial_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    old_governed boolean;
    new_governed boolean;
    conflicting_lineage_count integer;
BEGIN
    old_governed := systematic_fx.bar_pattern_experiment_is_governed(OLD.experiment_id);
    IF NOT old_governed THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        new_governed := systematic_fx.bar_pattern_experiment_is_governed(NEW.experiment_id);
        IF new_governed THEN
            RAISE EXCEPTION 'trials cannot be mutated into the governed bar-pattern catalog';
        END IF;
        RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'governed bar-pattern candidate trials are append-preserved';
    END IF;
    new_governed := systematic_fx.bar_pattern_experiment_is_governed(NEW.experiment_id);
    IF NOT new_governed THEN
        RAISE EXCEPTION 'governed bar-pattern candidate trials cannot leave their catalog';
    END IF;
    IF NEW.experiment_id IS DISTINCT FROM OLD.experiment_id
       OR NEW.trial_key IS DISTINCT FROM OLD.trial_key
       OR NEW.trial_type IS DISTINCT FROM OLD.trial_type
       OR NEW.parameters IS DISTINCT FROM OLD.parameters
       OR NEW.parameters_sha256 IS DISTINCT FROM OLD.parameters_sha256
       OR NEW.registered_at IS DISTINCT FROM OLD.registered_at THEN
        RAISE EXCEPTION 'governed bar-pattern candidate trial identity is immutable';
    END IF;
    IF OLD.status IN ('SUCCEEDED', 'REJECTED', 'FAILED', 'CANCELLED') THEN
        RAISE EXCEPTION 'terminal governed bar-pattern candidate trials are immutable';
    END IF;
    IF OLD.research_run_spec_id IS NOT NULL
       AND NEW.research_run_spec_id IS DISTINCT FROM OLD.research_run_spec_id THEN
        RAISE EXCEPTION 'governed bar-pattern candidate RunSpec binding is immutable';
    END IF;
    IF NEW.research_run_spec_id IS NOT NULL
       AND NOT systematic_fx.bar_pattern_run_spec_matches_trial(
           NEW.research_run_spec_id,
           NEW.experiment_trial_id
       ) THEN
        RAISE EXCEPTION 'governed bar-pattern candidate requires its exact RunSpec';
    END IF;
    IF NEW.status IS DISTINCT FROM OLD.status
       AND NOT (
           (OLD.status = 'REGISTERED'
            AND NEW.status IN ('RUNNING', 'SUCCEEDED', 'REJECTED', 'FAILED', 'CANCELLED'))
           OR (OLD.status = 'RUNNING'
               AND NEW.status IN ('SUCCEEDED', 'REJECTED', 'FAILED', 'CANCELLED'))
       ) THEN
        RAISE EXCEPTION 'invalid governed bar-pattern candidate lifecycle transition';
    END IF;
    IF NEW.status IS NOT DISTINCT FROM OLD.status
       AND (
           NEW.result_summary IS DISTINCT FROM OLD.result_summary
           OR NEW.started_at IS DISTINCT FROM OLD.started_at
           OR NEW.finished_at IS DISTINCT FROM OLD.finished_at
       ) THEN
        RAISE EXCEPTION 'active governed bar-pattern candidate lifecycle fields are immutable';
    END IF;

    IF NEW.status IN ('SUCCEEDED', 'REJECTED', 'FAILED', 'CANCELLED') THEN
        IF NEW.research_run_spec_id IS NULL THEN
            RAISE EXCEPTION 'terminal governed bar-pattern candidate requires a bound RunSpec';
        END IF;
        IF NEW.status NOT IN ('SUCCEEDED', 'REJECTED') THEN
            RAISE EXCEPTION 'bar-pattern candidate aborts fail attempts, not candidate trials';
        END IF;
        IF NEW.started_at IS NULL OR NEW.finished_at IS NULL
           OR (OLD.status = 'RUNNING'
               AND NEW.started_at IS DISTINCT FROM OLD.started_at) THEN
            RAISE EXCEPTION 'terminal bar-pattern candidate timestamps are invalid';
        END IF;
        IF NEW.result_summary #>> '{schema}' IS DISTINCT FROM
           'systematic_fx.bar_pattern_terminal_result.v1'
           OR NEW.result_summary #>> '{candidate_key}' IS DISTINCT FROM NEW.trial_key
           OR NEW.result_summary #>> '{attempt_status}' IS DISTINCT FROM 'SUCCEEDED'
           OR NEW.result_summary #>> '{trial_status}' IS DISTINCT FROM NEW.status
           OR NEW.result_summary #>> '{artifact,artifact_id}' IS NULL
           OR NEW.result_summary #>> '{compact_result,global_result_artifact_sha256}' IS NULL
           OR NEW.result_summary #>>
              '{compact_result,global_result_artifact_identity_sha256}' IS NULL
           OR NEW.result_summary #>> '{compact_result,evidence_manifest_sha256}' IS NULL
           OR NEW.result_summary #>>
              '{compact_result,evidence_artifact_identity_sha256}' IS NULL THEN
            RAISE EXCEPTION 'terminal bar-pattern candidate summary is incomplete';
        END IF;

        SELECT count(*)::integer
        INTO conflicting_lineage_count
        FROM systematic_fx.experiment_trials AS existing
        WHERE existing.experiment_id = NEW.experiment_id
          AND existing.experiment_trial_id <> NEW.experiment_trial_id
          AND existing.status IN ('SUCCEEDED', 'REJECTED')
          AND (
              existing.result_summary #>>
                  '{compact_result,global_result_artifact_sha256}'
                  IS DISTINCT FROM NEW.result_summary #>>
                  '{compact_result,global_result_artifact_sha256}'
              OR existing.result_summary #>>
                  '{compact_result,global_result_artifact_identity_sha256}'
                  IS DISTINCT FROM NEW.result_summary #>>
                  '{compact_result,global_result_artifact_identity_sha256}'
              OR existing.result_summary #>> '{compact_result,evidence_manifest_sha256}'
                  IS DISTINCT FROM NEW.result_summary #>>
                  '{compact_result,evidence_manifest_sha256}'
              OR existing.result_summary #>>
                  '{compact_result,evidence_artifact_identity_sha256}'
                  IS DISTINCT FROM NEW.result_summary #>>
                  '{compact_result,evidence_artifact_identity_sha256}'
          );
        IF conflicting_lineage_count <> 0 THEN
            RAISE EXCEPTION 'terminal bar-pattern candidates disagree on global evidence lineage';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER experiment_trials_protect_bar_pattern_lifecycle
BEFORE UPDATE OR DELETE ON systematic_fx.experiment_trials
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_bar_pattern_trial_lifecycle();

CREATE FUNCTION systematic_fx.enforce_bar_pattern_attempt_immediate()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    candidate_key text;
    candidate_trial_id bigint;
    candidate_trial_status text;
    candidate_trial_spec_id bigint;
    candidate_run_fingerprint text;
BEGIN
    SELECT run_spec.canonical_spec #>> '{parameters,bar_candidate_key}',
           run_spec.run_fingerprint
    INTO candidate_key, candidate_run_fingerprint
    FROM systematic_fx.research_run_specs AS run_spec
    JOIN systematic_fx.experiments AS experiment
      ON experiment.experiment_id = run_spec.experiment_id
    JOIN systematic_fx.campaigns AS campaign
      ON campaign.campaign_id = run_spec.campaign_id
    WHERE run_spec.research_run_spec_id = NEW.research_run_spec_id
      AND experiment.experiment_key =
          'bar_pattern_discovery_v1:experiment:frozen_candidate_catalog:v1'
      AND campaign.campaign_key = 'bar_pattern_discovery_v1';
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;
    IF candidate_key IS NULL OR btrim(candidate_key) = '' THEN
        RAISE EXCEPTION 'governed bar-pattern RunSpec lacks a canonical candidate key';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.status NOT IN ('QUEUED', 'SKIPPED_DUPLICATE') THEN
            RAISE EXCEPTION
                'governed bar-pattern attempts must be inserted as QUEUED or SKIPPED_DUPLICATE';
        END IF;
    ELSIF NOT (
        (OLD.status = 'QUEUED' AND NEW.status IN ('RUNNING', 'FAILED'))
        OR (OLD.status = 'RUNNING' AND NEW.status IN ('SUCCEEDED', 'FAILED'))
    ) THEN
        RAISE EXCEPTION 'invalid governed bar-pattern attempt lifecycle transition';
    END IF;

    SELECT trial.experiment_trial_id, trial.status, trial.research_run_spec_id
    INTO candidate_trial_id, candidate_trial_status, candidate_trial_spec_id
    FROM systematic_fx.experiment_trials AS trial
    JOIN systematic_fx.research_run_specs AS run_spec
      ON run_spec.experiment_id = trial.experiment_id
    WHERE run_spec.research_run_spec_id = NEW.research_run_spec_id
      AND trial.trial_key = candidate_key;
    IF candidate_trial_id IS NULL
       OR candidate_trial_spec_id IS DISTINCT FROM NEW.research_run_spec_id
       OR NOT systematic_fx.bar_pattern_run_spec_matches_trial(
           NEW.research_run_spec_id,
           candidate_trial_id
       ) THEN
        RAISE EXCEPTION 'bar-pattern attempt requires its prebound exact candidate trial';
    END IF;

    IF NEW.status IN ('QUEUED', 'RUNNING') THEN
        IF candidate_trial_status NOT IN ('REGISTERED', 'RUNNING')
           OR NEW.result_artifact_id IS NOT NULL
           OR NEW.trade_ledger_artifact_id IS NOT NULL
           OR NEW.reused_attempt_id IS NOT NULL
           OR NEW.result_summary IS DISTINCT FROM '{}'::jsonb
           OR NEW.error_message IS NOT NULL
           OR NEW.finished_at IS NOT NULL
           OR (NEW.status = 'QUEUED' AND NEW.started_at IS NOT NULL)
           OR (NEW.status = 'RUNNING'
               AND (NEW.started_at IS NULL OR OLD.started_at IS NOT NULL)) THEN
            RAISE EXCEPTION 'active bar-pattern attempt requires an active candidate trial';
        END IF;
    ELSIF NEW.status = 'FAILED' THEN
        IF candidate_trial_status NOT IN ('REGISTERED', 'RUNNING')
           OR NEW.result_artifact_id IS NOT NULL
           OR NEW.trade_ledger_artifact_id IS NOT NULL
           OR NEW.reused_attempt_id IS NOT NULL
           OR NEW.result_summary #>> '{candidate_key}' IS DISTINCT FROM candidate_key
           OR NEW.result_summary #>> '{run_fingerprint}'
              IS DISTINCT FROM candidate_run_fingerprint
           OR btrim(COALESCE(NEW.error_message, '')) = ''
           OR NEW.finished_at IS NULL
           OR (OLD.status = 'QUEUED' AND NEW.started_at IS NOT NULL)
           OR (OLD.status = 'RUNNING'
               AND (OLD.started_at IS NULL
                    OR NEW.started_at IS DISTINCT FROM OLD.started_at)) THEN
            RAISE EXCEPTION 'failed bar-pattern attempt has invalid candidate lineage';
        END IF;
    ELSIF NEW.status = 'SUCCEEDED' THEN
        IF candidate_trial_status NOT IN ('REGISTERED', 'RUNNING', 'SUCCEEDED', 'REJECTED')
           OR NEW.result_artifact_id IS NULL
           OR NEW.trade_ledger_artifact_id IS NOT NULL
           OR NEW.reused_attempt_id IS NOT NULL
           OR OLD.started_at IS NULL
           OR NEW.started_at IS DISTINCT FROM OLD.started_at
           OR NEW.finished_at IS NULL
           OR NEW.error_message IS NOT NULL
           OR NEW.result_summary #>> '{candidate_key}' IS DISTINCT FROM candidate_key
           OR NEW.result_summary #>> '{run_fingerprint}'
              IS DISTINCT FROM candidate_run_fingerprint
           OR NEW.result_summary #>> '{attempt_status}' IS DISTINCT FROM 'SUCCEEDED'
           OR NEW.result_summary #>> '{schema}' IS DISTINCT FROM
              'systematic_fx.bar_pattern_terminal_result.v1'
           OR NEW.result_summary #>> '{artifact,artifact_id}' IS DISTINCT FROM
              NEW.result_artifact_id::text
           OR NOT systematic_fx.bar_pattern_catalog_attempts_preregistered(
               NEW.research_run_spec_id
           )
           OR NOT EXISTS (
               SELECT 1
               FROM systematic_fx.artifacts AS terminal_artifact
               WHERE terminal_artifact.artifact_id = NEW.result_artifact_id
                 AND terminal_artifact.artifact_type = 'bar_terminal_result'
                 AND terminal_artifact.media_type = 'application/json'
                 AND terminal_artifact.artifact_key =
                     NEW.result_summary #>> '{artifact,artifact_key}'
                 AND terminal_artifact.sha256 =
                     NEW.result_summary #>> '{artifact,sha256}'
                 AND terminal_artifact.byte_size =
                     (NEW.result_summary #>> '{artifact,byte_size}')::bigint
                 AND terminal_artifact.metadata #>> '{artifact_identity_sha256}' =
                     NEW.result_summary #>> '{artifact,artifact_identity_sha256}'
                 AND terminal_artifact.metadata #>> '{content_sha256}' =
                     terminal_artifact.sha256
                 AND terminal_artifact.metadata #>> '{artifact_schema}' =
                     'systematic_fx.bar_terminal_result_artifact.v1'
                 AND terminal_artifact.metadata #>> '{artifact_type}' =
                     'bar_terminal_result'
                 AND terminal_artifact.metadata #> '{artifact_version}' = '1'::jsonb
                 AND terminal_artifact.metadata #> '{record_count}' = '484'::jsonb
                 AND terminal_artifact.metadata #>> '{schema_sha256}' =
                     '200dd0663c0100459eced9dc01f6ca59689444b37181610c9753fe3d18aeea8b'
                 AND terminal_artifact.metadata #>> '{source_manifest_sha256}' =
                     (
                         SELECT bound_trial.parameters #>>
                                    '{bar_dataset_manifest_sha256}'
                         FROM systematic_fx.experiment_trials AS bound_trial
                         WHERE bound_trial.experiment_trial_id = candidate_trial_id
                     )
                 AND terminal_artifact.metadata #>> '{root_kind}' = 'bar_patterns'
                 AND terminal_artifact.metadata #>> '{identity_schema}' =
                     'systematic_fx.bar_artifact_identity.v1'
                 AND terminal_artifact.metadata #>> '{artifact_identity_sha256}' =
                     systematic_fx.canonical_jsonb_sha256(
                         terminal_artifact.metadata
                             - 'artifact_identity_sha256' - 'content_sha256'
                     )
                 AND terminal_artifact.metadata #>> '{logical_identity,candidate_key}' =
                     candidate_key
                 AND terminal_artifact.metadata #>> '{logical_identity,run_fingerprint}' =
                     candidate_run_fingerprint
                 AND terminal_artifact.metadata #>>
                     '{logical_identity,campaign_definition_sha256}' =
                     (
                         SELECT bound_trial.parameters #>>
                                    '{campaign_definition_sha256}'
                         FROM systematic_fx.experiment_trials AS bound_trial
                         WHERE bound_trial.experiment_trial_id = candidate_trial_id
                     )
                 AND terminal_artifact.metadata #>>
                     '{logical_identity,bar_dataset_manifest_sha256}' =
                     terminal_artifact.metadata #>> '{source_manifest_sha256}'
                 AND terminal_artifact.metadata #>>
                     '{logical_identity,raw_source_manifest_sha256}' =
                     (
                         SELECT bound_trial.parameters #>> '{raw_source_manifest_sha256}'
                         FROM systematic_fx.experiment_trials AS bound_trial
                         WHERE bound_trial.experiment_trial_id = candidate_trial_id
                     )
                 AND terminal_artifact.metadata #>>
                     '{logical_identity,split_plan_sha256}' =
                     (
                         SELECT bound_trial.parameters #>> '{split_plan_sha256}'
                         FROM systematic_fx.experiment_trials AS bound_trial
                         WHERE bound_trial.experiment_trial_id = candidate_trial_id
                     )
                 AND terminal_artifact.metadata #>>
                     '{logical_identity,candidate_definition_sha256}' =
                     (
                         SELECT bound_trial.parameters #>>
                                    '{candidate_definition_sha256}'
                         FROM systematic_fx.experiment_trials AS bound_trial
                         WHERE bound_trial.experiment_trial_id = candidate_trial_id
                     )
                 AND terminal_artifact.metadata #>>
                     '{logical_identity,compact_result_sha256}' =
                     NEW.result_summary #>> '{compact_result_sha256}'
                 AND terminal_artifact.metadata #>> '{logical_identity,trial_status}' =
                     NEW.result_summary #>> '{trial_status}'
                 AND terminal_artifact.metadata #>> '{logical_identity,decision_label}' =
                     NEW.result_summary #>> '{decision_label}'
                 AND terminal_artifact.metadata #>> '{logical_identity,final_label}' =
                     NEW.result_summary #>> '{final_label}'
           ) THEN
            RAISE EXCEPTION 'succeeded bar-pattern attempt has invalid candidate lineage';
        END IF;
    ELSIF NEW.status = 'SKIPPED_DUPLICATE' THEN
        IF candidate_trial_status NOT IN ('SUCCEEDED', 'REJECTED')
           OR NEW.result_artifact_id IS NOT NULL
           OR NEW.trade_ledger_artifact_id IS NOT NULL
           OR NEW.started_at IS NOT NULL
           OR NEW.finished_at IS NULL
           OR NEW.error_message IS NOT NULL
           OR NEW.result_summary IS DISTINCT FROM jsonb_build_object(
               'reason', 'EXACT_FINGERPRINT_ALREADY_SUCCEEDED',
               'reused_attempt_id', NEW.reused_attempt_id
           ) THEN
            RAISE EXCEPTION 'duplicate bar-pattern attempt requires a terminal candidate trial';
        END IF;
    ELSE
        RAISE EXCEPTION 'bar-pattern attempts cannot use status %', NEW.status;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER research_run_attempts_enforce_bar_pattern_immediate
BEFORE INSERT OR UPDATE ON systematic_fx.research_run_attempts
FOR EACH ROW EXECUTE FUNCTION systematic_fx.enforce_bar_pattern_attempt_immediate();

CREATE FUNCTION systematic_fx.require_bar_pattern_terminal_consistency()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    target_spec_id bigint;
    candidate_key text;
    terminal_attempt_count integer;
    terminal_trial_count integer;
BEGIN
    IF TG_TABLE_NAME = 'research_run_attempts' THEN
        IF NEW.status IS DISTINCT FROM 'SUCCEEDED' THEN
            RETURN NULL;
        END IF;
        target_spec_id := NEW.research_run_spec_id;
    ELSE
        IF NEW.status NOT IN ('SUCCEEDED', 'REJECTED') THEN
            RETURN NULL;
        END IF;
        target_spec_id := NEW.research_run_spec_id;
    END IF;
    IF target_spec_id IS NULL THEN
        IF TG_TABLE_NAME = 'experiment_trials'
           AND systematic_fx.bar_pattern_experiment_is_governed(NEW.experiment_id) THEN
            RAISE EXCEPTION
                'terminal bar-pattern trial requires a prebound exact RunSpec';
        END IF;
        RETURN NULL;
    END IF;

    SELECT run_spec.canonical_spec #>> '{parameters,bar_candidate_key}'
    INTO candidate_key
    FROM systematic_fx.research_run_specs AS run_spec
    JOIN systematic_fx.campaigns AS campaign
      ON campaign.campaign_id = run_spec.campaign_id
    WHERE run_spec.research_run_spec_id = target_spec_id
      AND campaign.campaign_key = 'bar_pattern_discovery_v1';
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    SELECT count(*)::integer
    INTO terminal_attempt_count
    FROM systematic_fx.research_run_attempts AS attempt
    JOIN systematic_fx.experiment_trials AS trial
      ON trial.research_run_spec_id = attempt.research_run_spec_id
    WHERE attempt.research_run_spec_id = target_spec_id
      AND attempt.status = 'SUCCEEDED'
      AND trial.status IN ('SUCCEEDED', 'REJECTED')
      AND trial.trial_key = candidate_key
      AND attempt.result_artifact_id =
          (trial.result_summary #>> '{artifact,artifact_id}')::bigint
      AND attempt.result_summary = trial.result_summary;
    SELECT count(*)::integer
    INTO terminal_trial_count
    FROM systematic_fx.experiment_trials AS trial
    WHERE trial.research_run_spec_id = target_spec_id
      AND trial.status IN ('SUCCEEDED', 'REJECTED')
      AND trial.trial_key = candidate_key;
    IF terminal_attempt_count <> 1 OR terminal_trial_count <> 1 THEN
        RAISE EXCEPTION
            'terminal bar-pattern RunSpec requires one atomic exact attempt/trial pair';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER research_run_attempts_require_bar_pattern_terminal_pair
AFTER INSERT OR UPDATE ON systematic_fx.research_run_attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
WHEN (NEW.status = 'SUCCEEDED')
EXECUTE FUNCTION systematic_fx.require_bar_pattern_terminal_consistency();

CREATE CONSTRAINT TRIGGER experiment_trials_require_bar_pattern_terminal_pair
AFTER INSERT OR UPDATE ON systematic_fx.experiment_trials
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
WHEN (NEW.status IN ('SUCCEEDED', 'REJECTED'))
EXECUTE FUNCTION systematic_fx.require_bar_pattern_terminal_consistency();

CREATE FUNCTION systematic_fx.protect_bar_pattern_artifact_lineage()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    old_protected boolean;
    new_protected boolean;
BEGIN
    old_protected := OLD.artifact_type IN (
        'bar_registration',
        'bar_code_snapshot',
        'bar_discovery_matches_shard',
        'bar_discovery_replays_shard',
        'bar_discovery_evidence_manifest',
        'bar_global_discovery_result',
        'bar_terminal_result'
    ) OR (
        OLD.metadata #>> '{identity_schema}' = 'systematic_fx.bar_artifact_identity.v1'
        AND OLD.metadata #>> '{root_kind}' = 'bar_patterns'
    );
    IF old_protected THEN
        RAISE EXCEPTION 'bar-pattern evidence and result artifacts are immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    new_protected := NEW.artifact_type IN (
        'bar_registration',
        'bar_code_snapshot',
        'bar_discovery_matches_shard',
        'bar_discovery_replays_shard',
        'bar_discovery_evidence_manifest',
        'bar_global_discovery_result',
        'bar_terminal_result'
    ) OR (
        NEW.metadata #>> '{identity_schema}' = 'systematic_fx.bar_artifact_identity.v1'
        AND NEW.metadata #>> '{root_kind}' = 'bar_patterns'
    );
    IF new_protected THEN
        RAISE EXCEPTION 'artifacts cannot be mutated into protected bar-pattern identity';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER artifacts_protect_bar_pattern_lineage
BEFORE UPDATE OR DELETE ON systematic_fx.artifacts
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_bar_pattern_artifact_lineage();

COMMENT ON FUNCTION systematic_fx.bar_pattern_run_spec_matches_trial(bigint, bigint) IS
    'Checks the immutable candidate, experiment, campaign, and canonical RunSpec edge.';
COMMENT ON TRIGGER experiment_trials_protect_bar_pattern_lifecycle
ON systematic_fx.experiment_trials IS
    'Prevents RunSpec rebinding and terminal re-entry for frozen bar-pattern candidates.';
COMMENT ON TRIGGER research_run_attempts_enforce_bar_pattern_immediate
ON systematic_fx.research_run_attempts IS
    'Immediately rejects bar attempts unless the complete frozen RunSpec is prebound.';
COMMENT ON TRIGGER research_run_attempts_require_bar_pattern_terminal_pair
ON systematic_fx.research_run_attempts IS
    'Defers only the atomic succeeded-attempt and terminal-trial consistency check.';
COMMENT ON TRIGGER artifacts_protect_bar_pattern_lineage
ON systematic_fx.artifacts IS
    'Makes bar-pattern registration, code, evidence, global, and terminal artifacts immutable.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (22, 'bar_pattern_registry_governance', :'migration_checksum');

COMMIT;
