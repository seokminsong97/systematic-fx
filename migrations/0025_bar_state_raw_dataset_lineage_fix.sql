BEGIN;

CREATE OR REPLACE FUNCTION systematic_fx.bar_state_run_spec_matches_trial(
    target_spec_id bigint,
    target_trial_id bigint
)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM systematic_fx.experiment_trials AS trial
        JOIN systematic_fx.experiments AS experiment
          ON experiment.experiment_id = trial.experiment_id
        JOIN systematic_fx.campaigns AS campaign
          ON campaign.campaign_id = experiment.campaign_id
        JOIN systematic_fx.datasets AS dataset
          ON dataset.dataset_id = campaign.dataset_id
        JOIN systematic_fx.research_run_specs AS run_spec
          ON run_spec.research_run_spec_id = target_spec_id
         AND run_spec.campaign_id = campaign.campaign_id
         AND run_spec.experiment_id = experiment.experiment_id
        JOIN systematic_fx.artifacts AS registration_artifact
          ON registration_artifact.artifact_id = experiment.registration_artifact_id
        WHERE trial.experiment_trial_id = target_trial_id
          AND campaign.campaign_key = 'bar_state_conditional_v2'
          AND campaign.name = 'Frozen conditional candle-state Discovery v2'
          AND campaign.status = 'FROZEN'
          AND campaign.frozen_at IS NOT NULL
          AND campaign.holdout_revealed_at IS NULL
          AND campaign.closed_at IS NULL
          AND campaign.data_manifest_sha256 =
              'e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc'
          AND campaign.feature_version = 'bar_state_features_v1'
          AND campaign.outcome_version = 'bar_state_twenty_day_first_touch_labels_v1'
          AND campaign.cost_model_version = 'BAR_TRADE_ONLY_COSTS_V1'
          AND campaign.execution_model_version = 'bar_state_next_open_49_cell_replay_v1'
          AND campaign.trial_budget = 12
          AND campaign.finalist_budget = 4
          AND campaign.split_policy #>> '{authorized_stage}' = 'DISCOVERY_ONLY'
          AND campaign.split_policy #>> '{split_plan_sha256}' =
              '5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043'
          AND campaign.split_policy #>> '{raw_source_manifest_sha256}' =
              '14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de'
          AND campaign.split_policy #>> '{bar_dataset_manifest_sha256}' =
              campaign.data_manifest_sha256
          AND experiment.experiment_key =
              'bar_state_conditional_v2:experiment:frozen_candidate_catalog:v1'
          AND experiment.status = 'FROZEN'
          AND experiment.frozen_at IS NOT NULL
          AND experiment.completed_at IS NULL
          AND experiment.primary_family = 'CONDITIONAL_BAR_STATE_MODEL'
          AND experiment.model_family = 'ELASTIC_NET_MULTINOMIAL_LOGISTIC'
          AND experiment.direction = 'BOTH'
          AND experiment.trial_budget = 12
          AND experiment.trials_registered = 12
          AND registration_artifact.artifact_type = 'bar_state_conditional_v2'
          AND registration_artifact.metadata #>> '{artifact_schema}' =
              'systematic_fx.bar_state_registration_artifact.v1'
          AND registration_artifact.metadata #>>
                  '{logical_identity,artifact_kind}' = 'REGISTRATION'
          AND trial.trial_type = 'MODEL_FIT'
          AND trial.trial_key ~
              '^bsv2_tf(0300|1800)_fs(morphology|state)_cm(005|010|015)$'
          AND trial.parameters_sha256 =
              systematic_fx.canonical_jsonb_sha256(trial.parameters)
          AND systematic_fx.jsonb_has_exact_keys(
              trial.parameters,
              ARRAY[
                  'bar_dataset_manifest_sha256', 'campaign_definition',
                  'campaign_definition_sha256', 'candidate_catalog_sha256',
                  'candidate_definition', 'candidate_definition_sha256',
                  'candidate_key', 'config_file_sha256',
                  'config_semantic_sha256', 'discovery_scope',
                  'discovery_scope_sha256', 'raw_source_manifest_sha256',
                  'schema', 'split_plan', 'split_plan_sha256',
                  'training_plan', 'training_plan_sha256'
              ]
          )
          AND trial.parameters #>> '{schema}' =
              'systematic_fx.bar_state_trial_parameters.v1'
          AND trial.parameters #>> '{candidate_key}' = trial.trial_key
          AND trial.parameters #>> '{candidate_definition,candidate_key}' = trial.trial_key
          AND trial.parameters #>> '{candidate_definition_sha256}' =
              systematic_fx.canonical_jsonb_sha256(
                  trial.parameters -> 'candidate_definition'
              )
          AND trial.parameters #>> '{candidate_definition_sha256}' = CASE trial.trial_key
              WHEN 'bsv2_tf0300_fsmorphology_cm005' THEN
                  'eda245aa4a2d3e892936800ad41225fbfd5a1dfde353a209a9d1c6f3f101b74e'
              WHEN 'bsv2_tf0300_fsmorphology_cm010' THEN
                  'bd12fcd5d0c1b3326253dbd039784277997cb4d4675736041a8834ee99ef25df'
              WHEN 'bsv2_tf0300_fsmorphology_cm015' THEN
                  '932e05ad48d947810dc496befc34022b35dee06da27216b44560ddcfcb546e11'
              WHEN 'bsv2_tf0300_fsstate_cm005' THEN
                  '9627234536189a0542a6a8c53f3b4164c75b9afaab3e62dfa25fcc7a76ba36ec'
              WHEN 'bsv2_tf0300_fsstate_cm010' THEN
                  '49ac20b55570d00d3f59ec375c8993b2d6e118eaacded73d14677d84cdc3b2ed'
              WHEN 'bsv2_tf0300_fsstate_cm015' THEN
                  '50e3a9eb79b5593388df290c81852a99ecfef4e408a50028cbde28d9692d2f66'
              WHEN 'bsv2_tf1800_fsmorphology_cm005' THEN
                  '66fc50548b7c5dfbc7a4bf244b300aaac438a99f221dd2376ba5387ef9142857'
              WHEN 'bsv2_tf1800_fsmorphology_cm010' THEN
                  'b51bf381e371266cf239e3cfdeda828fb2569d06dc238188ffb32d2dead25f75'
              WHEN 'bsv2_tf1800_fsmorphology_cm015' THEN
                  '8975d05c1ba0ceb6645fa4ab1f1707835d7e2468dc8d5d97ea99e2ddfadfeb64'
              WHEN 'bsv2_tf1800_fsstate_cm005' THEN
                  'f9450de0bd96102d15ca946331669a7951a837c173f60ade4e8de8cbdba0c031'
              WHEN 'bsv2_tf1800_fsstate_cm010' THEN
                  '6c6cfd1373d36573e5214f9f2d84e0a06f62ac67d8a8385c6ae2e802713e998b'
              WHEN 'bsv2_tf1800_fsstate_cm015' THEN
                  '172e8f5364dfb3b3d071320b291b16228b598ac4f0b639850b335819b0332faf'
              ELSE NULL
          END
          AND trial.parameters #>> '{campaign_definition_sha256}' =
              systematic_fx.canonical_jsonb_sha256(
                  trial.parameters -> 'campaign_definition'
              )
          AND trial.parameters #>> '{campaign_definition_sha256}' =
              '4502e2ec1c40f344fce27066223a25e6b2f7456736e09fe0d96faab4171134f9'
          AND trial.parameters #>> '{candidate_catalog_sha256}' =
              '3e24dc08e9027ec604b5ab433368a54c4f7a4c89577599b79de372f62262120d'
          AND trial.parameters #>> '{config_file_sha256}' =
              '8408a349ac2cd595e2104201185b361a5a58c7b24182babafe29e66f5c93a6e9'
          AND trial.parameters #>> '{config_semantic_sha256}' =
              '7b2d5a1e70d59b97e699d0ee479670937975ba5bcd73bc003211a1bb856e84ba'
          AND trial.parameters #>> '{training_plan_sha256}' =
              systematic_fx.canonical_jsonb_sha256(
                  trial.parameters -> 'training_plan'
              )
          AND trial.parameters #>> '{training_plan_sha256}' =
              '9a4833aa53fe03788ddf224efcd24abbcc492498b915f174b6473f9c37f3fc8a'
          AND trial.parameters #>> '{split_plan_sha256}' =
              systematic_fx.canonical_jsonb_sha256(
                  trial.parameters -> 'split_plan'
              )
          AND trial.parameters #>> '{split_plan_sha256}' =
              '5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043'
          AND trial.parameters #>> '{discovery_scope_sha256}' =
              systematic_fx.canonical_jsonb_sha256(
                  trial.parameters -> 'discovery_scope'
              )
          AND trial.parameters #>> '{discovery_scope_sha256}' =
              '35e59a3475d9e79e17e1b132b6a2044458f46069a715ceb7c458ad298cab3ec0'
          AND trial.parameters #>> '{discovery_scope,result_visibility}' = 'VISIBLE'
          AND trial.parameters #>> '{discovery_scope,split_key}' = 'discovery'
          AND trial.parameters #>> '{raw_source_manifest_sha256}' =
              '14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de'
          AND trial.parameters #>> '{bar_dataset_manifest_sha256}' =
              campaign.data_manifest_sha256
          AND dataset.dataset_key = 'glbx_mdp3_mbp_10_6e_fut_v1'
          AND dataset.manifest_sha256 = trial.parameters #>> '{raw_source_manifest_sha256}'
          AND dataset.status NOT IN ('REJECTED', 'RETIRED')
          AND run_spec.run_kind = 'MODEL_FIT'
          AND run_spec.engine_version = 'bar_state_conditional_discovery_v2'
          AND run_spec.direction = 'BOTH'
          AND run_spec.deterministic_seed = 20260809
          AND run_spec.eligible_calendar_version =
              'bar_dataset_eligible_calendar_v1'
          AND run_spec.eligible_calendar_sha256 =
              'a8b57ad2ffcb68accc0e792c08082cf51090b87bf963800178f88dd27af9da14'
          AND run_spec.split_version = 'bar_state_discovery_inner_oos_v1'
          AND run_spec.split_sha256 =
              trial.parameters #>> '{training_plan_sha256}'
          AND run_spec.feature_version = 'bar_state_features_v1'
          AND run_spec.feature_sha256 =
              systematic_fx.canonical_jsonb_sha256(
                  trial.parameters #> '{candidate_definition,feature_policy}'
              )
          AND run_spec.outcome_version =
              'bar_state_twenty_day_first_touch_labels_v1'
          AND run_spec.outcome_sha256 =
              systematic_fx.canonical_jsonb_sha256(
                  trial.parameters #> '{candidate_definition,label_policy}'
              )
          AND run_spec.cost_version = 'BAR_TRADE_ONLY_COSTS_V1'
          AND run_spec.cost_sha256 =
              systematic_fx.canonical_jsonb_sha256(
                  trial.parameters #> '{candidate_definition,cost_model}'
              )
          AND run_spec.execution_version = 'bar_state_next_open_49_cell_replay_v1'
          AND run_spec.execution_sha256 = systematic_fx.canonical_jsonb_sha256(
              jsonb_build_object(
                  'economic_barrier_policy',
                      trial.parameters #> '{candidate_definition,economic_barrier_policy}',
                  'entry_policy',
                      trial.parameters #> '{candidate_definition,entry_policy}',
                  'prediction_policy',
                      trial.parameters #> '{candidate_definition,prediction_policy}'
              )
          )
          AND run_spec.source_manifest_hashes = jsonb_build_object(
              'raw_mbp10_source_manifest_v1',
                  trial.parameters -> 'raw_source_manifest_sha256',
              'selected_trade_bar_dataset_manifest_v1',
                  trial.parameters -> 'bar_dataset_manifest_sha256'
          )
          AND run_spec.code_commit = campaign.code_commit
          AND run_spec.code_snapshot_sha256 = registration_artifact.metadata #>>
              '{logical_identity,lineage,code_snapshot_sha256}'
          AND run_spec.dependency_lock_sha256 = registration_artifact.metadata #>>
              '{logical_identity,lineage,dependency_lock_sha256}'
          AND systematic_fx.canonical_jsonb_sha256(
                  run_spec.canonical_spec -> 'runtime_environment'
              ) = registration_artifact.metadata #>>
                  '{logical_identity,lineage,runtime_environment_sha256}'
          AND run_spec.canonical_spec -> 'signal_policy' = jsonb_build_object(
              'authorized_stage', 'DISCOVERY_ONLY',
              'candidate_key', trial.trial_key,
              'feature_policy',
                  trial.parameters #> '{candidate_definition,feature_policy}',
              'prediction_policy',
                  trial.parameters #> '{candidate_definition,prediction_policy}',
              'schema', 'systematic_fx.bar_state_signal_policy.v1'
          )
          AND run_spec.canonical_spec -> 'entry_policy' =
              trial.parameters #> '{candidate_definition,entry_policy}'
          AND run_spec.canonical_spec -> 'barrier_policy' =
              trial.parameters #> '{candidate_definition,economic_barrier_policy}'
          AND run_spec.canonical_spec -> 'terminal_policy' = jsonb_build_object(
              'authorized_stage', 'DISCOVERY_ONLY',
              'boundary_event_ordering',
                  trial.parameters #>>
                      '{candidate_definition,label_policy,boundary_event_ordering}',
              'contract_boundary_policy',
                  trial.parameters #>>
                      '{candidate_definition,label_policy,contract_boundary_policy}',
              'observation_horizon_active_days',
                  trial.parameters #>
                      '{candidate_definition,label_policy,observation_horizon_active_days}',
              'quality_boundary_policy',
                  trial.parameters #>>
                      '{candidate_definition,label_policy,quality_boundary_policy}',
              'split_boundary_policy', 'DISCOVERY_OUTCOME_TAIL_END'
          )
          AND run_spec.canonical_spec #>>
                  '{terminal_policy,boundary_event_ordering}' =
              'UNRESOLVED_AT_BOUNDARY_CENSORED_PRIOR_FIRST_TOUCH_PRESERVED'
          AND run_spec.canonical_spec #>> '{parameters,authorized_stage}' =
              'DISCOVERY_ONLY'
          AND run_spec.canonical_spec #>> '{parameters,bar_state_candidate_key}' =
              trial.trial_key
          AND run_spec.canonical_spec #>>
                  '{parameters,bar_state_candidate_definition_sha256}' =
              trial.parameters #>> '{candidate_definition_sha256}'
          AND run_spec.canonical_spec #>>
                  '{parameters,bar_state_candidate_catalog_sha256}' =
              trial.parameters #>> '{candidate_catalog_sha256}'
          AND run_spec.canonical_spec #>>
                  '{parameters,bar_state_config_file_sha256}' =
              trial.parameters #>> '{config_file_sha256}'
          AND run_spec.canonical_spec #>>
                  '{parameters,bar_state_config_semantic_sha256}' =
              trial.parameters #>> '{config_semantic_sha256}'
          AND run_spec.canonical_spec #>>
                  '{parameters,bar_state_discovery_scope_sha256}' =
              trial.parameters #>> '{discovery_scope_sha256}'
          AND run_spec.canonical_spec #>>
                  '{parameters,bar_state_training_plan_sha256}' =
              trial.parameters #>> '{training_plan_sha256}'
          AND run_spec.canonical_spec #>>
                  '{parameters,bar_state_trial_parameters_sha256}' =
              trial.parameters_sha256
    );
$$;

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (25, 'bar_state_raw_dataset_lineage_fix', :'migration_checksum');

COMMIT;
