BEGIN;

CREATE OR REPLACE FUNCTION systematic_fx.bar_state_governance_profile(
    target_campaign_key text
)
RETURNS TABLE (
    profile_version text,
    campaign_key text,
    campaign_name text,
    experiment_key text,
    artifact_type text,
    engine_version text,
    config_file_sha256 text,
    config_semantic_sha256 text,
    candidate_catalog_sha256 text,
    campaign_definition_sha256 text,
    model_policy_sha256 text,
    model_max_iter integer,
    candidate_definition_sha256_by_key jsonb,
    amends_campaign_key text,
    predecessor_campaign_definition_sha256 text,
    predecessor_code_commit text,
    predecessor_gate_policy text
)
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog
AS $$
    SELECT profile.*
    FROM (
        VALUES
        (
            'V2'::text,
            'bar_state_conditional_v2'::text,
            'Frozen conditional candle-state Discovery v2'::text,
            'bar_state_conditional_v2:experiment:frozen_candidate_catalog:v1'::text,
            'bar_state_conditional_v2'::text,
            'bar_state_conditional_discovery_v2'::text,
            '8408a349ac2cd595e2104201185b361a5a58c7b24182babafe29e66f5c93a6e9'::text,
            '7b2d5a1e70d59b97e699d0ee479670937975ba5bcd73bc003211a1bb856e84ba'::text,
            '3e24dc08e9027ec604b5ab433368a54c4f7a4c89577599b79de372f62262120d'::text,
            '4502e2ec1c40f344fce27066223a25e6b2f7456736e09fe0d96faab4171134f9'::text,
            '04c8ef9a56632dcb9fe9b5259716e351395dcda3cd422e6e614e136f46bcb9ac'::text,
            5000,
            jsonb_build_object(
                'bsv2_tf0300_fsmorphology_cm005', 'eda245aa4a2d3e892936800ad41225fbfd5a1dfde353a209a9d1c6f3f101b74e',
                'bsv2_tf0300_fsmorphology_cm010', 'bd12fcd5d0c1b3326253dbd039784277997cb4d4675736041a8834ee99ef25df',
                'bsv2_tf0300_fsmorphology_cm015', '932e05ad48d947810dc496befc34022b35dee06da27216b44560ddcfcb546e11',
                'bsv2_tf0300_fsstate_cm005', '9627234536189a0542a6a8c53f3b4164c75b9afaab3e62dfa25fcc7a76ba36ec',
                'bsv2_tf0300_fsstate_cm010', '49ac20b55570d00d3f59ec375c8993b2d6e118eaacded73d14677d84cdc3b2ed',
                'bsv2_tf0300_fsstate_cm015', '50e3a9eb79b5593388df290c81852a99ecfef4e408a50028cbde28d9692d2f66',
                'bsv2_tf1800_fsmorphology_cm005', '66fc50548b7c5dfbc7a4bf244b300aaac438a99f221dd2376ba5387ef9142857',
                'bsv2_tf1800_fsmorphology_cm010', 'b51bf381e371266cf239e3cfdeda828fb2569d06dc238188ffb32d2dead25f75',
                'bsv2_tf1800_fsmorphology_cm015', '8975d05c1ba0ceb6645fa4ab1f1707835d7e2468dc8d5d97ea99e2ddfadfeb64',
                'bsv2_tf1800_fsstate_cm005', 'f9450de0bd96102d15ca946331669a7951a837c173f60ade4e8de8cbdba0c031',
                'bsv2_tf1800_fsstate_cm010', '6c6cfd1373d36573e5214f9f2d84e0a06f62ac67d8a8385c6ae2e802713e998b',
                'bsv2_tf1800_fsstate_cm015', '172e8f5364dfb3b3d071320b291b16228b598ac4f0b639850b335819b0332faf'
            ),
            NULL::text,
            NULL::text,
            NULL::text,
            NULL::text
        ),
        (
            'V2A'::text,
            'bar_state_conditional_v2a'::text,
            'Frozen conditional candle-state Discovery v2a'::text,
            'bar_state_conditional_v2a:experiment:frozen_candidate_catalog:v1'::text,
            'bar_state_conditional_v2a'::text,
            'bar_state_conditional_discovery_v2a'::text,
            'ecc4837c67e1c42ae69bfe0c74744e8aba9ba7cd99584b2dc0c091f6579f0a52'::text,
            '2e2e3c6ee68af86fffa864ce736c24802eea7901a63d4ebda583327df06f156a'::text,
            '97bbdacd0d655a1ca4e81085f3f25fb32da0bf31329bbd670ba89778611084d6'::text,
            '8a332ad6998bb8bf48c3de94bc0ca660905a08acb848580ee5e31d9c42f8033c'::text,
            '844cd3964e2871fecd13b7f7a76f07016b150b853c290c4188e275cd2226874f'::text,
            50000,
            jsonb_build_object(
                'bsv2_tf0300_fsmorphology_cm005', 'ef9d158d5909beaee7727aa5c71c99be2c44053399325c0438f508cfa0742eda',
                'bsv2_tf0300_fsmorphology_cm010', '62fa347ad4d2824e3220df29834bf4bfedd58d9df16c161b95fbfd2ab36defb7',
                'bsv2_tf0300_fsmorphology_cm015', '2620affd7d6bc99001b667d52173d90b24ee379134c6053ed27f6cad52ee4d6a',
                'bsv2_tf0300_fsstate_cm005', '315c7ac44d828afe96f4a3ec2eb38e047fe7a2e7c9c268dabe01f557807383ac',
                'bsv2_tf0300_fsstate_cm010', '6d8c80b71bccb9d25c69a173585c9dfe47a888a0fe5918240f0e95063d69035b',
                'bsv2_tf0300_fsstate_cm015', 'b8530e604700b64a8e39cee7e4c6719bfd1294c8f4c64e25345a731442301ec0',
                'bsv2_tf1800_fsmorphology_cm005', 'eb5404c6a507b05d243fdb1e81aa8ab9a93cb0a3bc958321b2a12a03600e44ee',
                'bsv2_tf1800_fsmorphology_cm010', '375d9a388e1346b3557703beee061c408371683b1aa27c2d7b6fa8862ea298da',
                'bsv2_tf1800_fsmorphology_cm015', '0367e3821e20fe2eb07ec278a3d3faff2bf90e15c8d1c2b1de241763ee5cf7d3',
                'bsv2_tf1800_fsstate_cm005', 'a98c2d8e60da3ffc8dbf84461d0873627dfbec47847891f23c44a6785685ae1e',
                'bsv2_tf1800_fsstate_cm010', '57f4d5577456ff4ca3f30d82bb731b07c5638fa1b5f4a86b26d039d954bd19a3',
                'bsv2_tf1800_fsstate_cm015', '696f5eac1caa452082cb51c0aef9c0f856daa96e31e89267b5d05f081242ef91'
            ),
            'bar_state_conditional_v2'::text,
            '4502e2ec1c40f344fce27066223a25e6b2f7456736e09fe0d96faab4171134f9'::text,
            '2ca2b0b6158c1d1e9d880c2ed65ec7d7582de189'::text,
            'REQUIRE_EXACT_FAILED_PREDECESSOR_WITH_NO_OOS_EVIDENCE'::text
        )
    ) AS profile(
        profile_version, campaign_key, campaign_name, experiment_key,
        artifact_type, engine_version, config_file_sha256,
        config_semantic_sha256, candidate_catalog_sha256,
        campaign_definition_sha256, model_policy_sha256, model_max_iter,
        candidate_definition_sha256_by_key, amends_campaign_key,
        predecessor_campaign_definition_sha256, predecessor_code_commit,
        predecessor_gate_policy
    )
    WHERE profile.campaign_key = target_campaign_key;
$$;

CREATE OR REPLACE FUNCTION systematic_fx.bar_state_v2a_predecessor_is_clean()
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
    WITH predecessor AS (
        SELECT campaign.campaign_id, experiment.experiment_id
        FROM systematic_fx.campaigns AS campaign
        JOIN systematic_fx.datasets AS dataset
          ON dataset.dataset_id = campaign.dataset_id
        JOIN systematic_fx.experiments AS experiment
          ON experiment.campaign_id = campaign.campaign_id
        JOIN systematic_fx.artifacts AS registration_artifact
          ON registration_artifact.artifact_id = experiment.registration_artifact_id
        JOIN LATERAL systematic_fx.bar_state_governance_profile(
            campaign.campaign_key
        ) AS profile ON profile.profile_version = 'V2'
        WHERE campaign.campaign_key = 'bar_state_conditional_v2'
          AND campaign.name = profile.campaign_name
          AND campaign.status = 'FROZEN'
          AND campaign.frozen_at IS NOT NULL
          AND campaign.holdout_revealed_at IS NULL
          AND campaign.closed_at IS NULL
          AND campaign.code_commit =
              '2ca2b0b6158c1d1e9d880c2ed65ec7d7582de189'
          AND campaign.config_sha256 = profile.campaign_definition_sha256
          AND campaign.data_manifest_sha256 =
              'e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc'
          AND campaign.feature_version = 'bar_state_features_v1'
          AND campaign.outcome_version =
              'bar_state_twenty_day_first_touch_labels_v1'
          AND campaign.cost_model_version = 'BAR_TRADE_ONLY_COSTS_V1'
          AND campaign.execution_model_version =
              'bar_state_next_open_49_cell_replay_v1'
          AND campaign.trial_budget = 12
          AND campaign.finalist_budget = 4
          AND campaign.split_policy #>> '{authorized_stage}' = 'DISCOVERY_ONLY'
          AND campaign.split_policy #>> '{split_plan_sha256}' =
              '5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043'
          AND campaign.split_policy #>> '{raw_source_manifest_sha256}' =
              '14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de'
          AND campaign.split_policy #>> '{bar_dataset_manifest_sha256}' =
              campaign.data_manifest_sha256
          AND dataset.dataset_key = 'glbx_mdp3_mbp_10_6e_fut_v1'
          AND dataset.manifest_sha256 =
              '14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de'
          AND dataset.status NOT IN ('REJECTED', 'RETIRED')
          AND experiment.experiment_key = profile.experiment_key
          AND experiment.status = 'FROZEN'
          AND experiment.frozen_at IS NOT NULL
          AND experiment.completed_at IS NULL
          AND experiment.primary_family = 'CONDITIONAL_BAR_STATE_MODEL'
          AND experiment.model_family = 'ELASTIC_NET_MULTINOMIAL_LOGISTIC'
          AND experiment.direction = 'BOTH'
          AND experiment.trial_budget = 12
          AND experiment.trials_registered = 12
          AND experiment.code_commit = campaign.code_commit
          AND registration_artifact.artifact_key =
              profile.artifact_type || ':registration:' ||
              profile.campaign_definition_sha256
          AND registration_artifact.artifact_type = profile.artifact_type
          AND registration_artifact.metadata #>> '{artifact_schema}' =
              'systematic_fx.bar_state_registration_artifact.v1'
          AND registration_artifact.metadata #>>
                  '{logical_identity,artifact_kind}' = 'REGISTRATION'
          AND registration_artifact.metadata #>>
                  '{logical_identity,campaign_key}' = profile.campaign_key
          AND registration_artifact.metadata #>>
                  '{logical_identity,campaign_definition_sha256}' =
              profile.campaign_definition_sha256
          AND registration_artifact.metadata #>>
                  '{logical_identity,candidate_catalog_sha256}' =
              profile.candidate_catalog_sha256
          AND registration_artifact.metadata #>>
                  '{logical_identity,lineage,config_file_sha256}' =
              profile.config_file_sha256
          AND registration_artifact.metadata #>>
                  '{logical_identity,lineage,config_semantic_sha256}' =
              profile.config_semantic_sha256
          AND registration_artifact.metadata #>>
                  '{logical_identity,lineage,candidate_catalog_sha256}' =
              profile.candidate_catalog_sha256
    )
    SELECT EXISTS (
        SELECT 1
        FROM predecessor
        WHERE (
            SELECT count(*)
            FROM systematic_fx.experiment_trials AS trial
            WHERE trial.experiment_id = predecessor.experiment_id
        ) = 12
          AND (
            SELECT count(*)
            FROM systematic_fx.experiment_trials AS trial
            WHERE trial.experiment_id = predecessor.experiment_id
              AND trial.status = 'REGISTERED'
              AND trial.research_run_spec_id IS NOT NULL
              AND systematic_fx.bar_state_run_spec_matches_trial(
                  trial.research_run_spec_id,
                  trial.experiment_trial_id
              )
          ) = 12
          AND (
            SELECT count(DISTINCT trial.trial_key)
            FROM systematic_fx.experiment_trials AS trial
            WHERE trial.experiment_id = predecessor.experiment_id
          ) = 12
          AND (
            SELECT count(DISTINCT trial.research_run_spec_id)
            FROM systematic_fx.experiment_trials AS trial
            WHERE trial.experiment_id = predecessor.experiment_id
          ) = 12
          AND (
            SELECT count(*)
            FROM systematic_fx.research_run_specs AS run_spec
            WHERE run_spec.experiment_id = predecessor.experiment_id
          ) = 12
          AND (
            SELECT count(*)
            FROM systematic_fx.research_run_attempts AS attempt
            JOIN systematic_fx.research_run_specs AS run_spec
              ON run_spec.research_run_spec_id = attempt.research_run_spec_id
            WHERE run_spec.campaign_id = predecessor.campaign_id
              AND run_spec.experiment_id = predecessor.experiment_id
          ) = 12
          AND (
            SELECT count(*)
            FROM systematic_fx.research_run_attempts AS attempt
            JOIN systematic_fx.research_run_specs AS run_spec
              ON run_spec.research_run_spec_id = attempt.research_run_spec_id
            WHERE run_spec.campaign_id = predecessor.campaign_id
              AND run_spec.experiment_id = predecessor.experiment_id
              AND attempt.status = 'FAILED'
              AND attempt.attempt_number = 1
              AND attempt.result_artifact_id IS NULL
              AND attempt.trade_ledger_artifact_id IS NULL
              AND attempt.reused_attempt_id IS NULL
              AND attempt.started_at IS NOT NULL
              AND attempt.finished_at IS NOT NULL
              AND btrim(COALESCE(attempt.error_message, '')) <> ''
          ) = 12
          AND (
            SELECT count(DISTINCT attempt.research_run_spec_id)
            FROM systematic_fx.research_run_attempts AS attempt
            JOIN systematic_fx.research_run_specs AS run_spec
              ON run_spec.research_run_spec_id = attempt.research_run_spec_id
            WHERE run_spec.campaign_id = predecessor.campaign_id
              AND run_spec.experiment_id = predecessor.experiment_id
          ) = 12
          AND NOT EXISTS (
            SELECT 1
            FROM systematic_fx.bar_state_artifact_links AS link
            WHERE link.campaign_id = predecessor.campaign_id
              AND link.artifact_role IN (
                  'FEATURE', 'LABEL', 'MODEL', 'OOS_TRADE',
                  'GLOBAL_RESULT', 'TERMINAL_RESULT'
              )
          )
    );
$$;

CREATE OR REPLACE FUNCTION systematic_fx.bar_state_experiment_is_governed(
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
        JOIN LATERAL systematic_fx.bar_state_governance_profile(
            campaign.campaign_key
        ) AS profile ON profile.experiment_key = experiment.experiment_key
        WHERE experiment.experiment_id = target_experiment_id
    );
$$;

CREATE OR REPLACE FUNCTION systematic_fx.enforce_bar_state_v2a_predecessor_campaign()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NEW.campaign_key = 'bar_state_conditional_v2a' THEN
        PERFORM campaign.campaign_id
        FROM systematic_fx.campaigns AS campaign
        WHERE campaign.campaign_key = 'bar_state_conditional_v2'
        FOR UPDATE;
        IF NOT FOUND
           OR systematic_fx.bar_state_v2a_predecessor_is_clean()
                IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'State V2A requires its exact clean failed V2 predecessor';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER campaigns_require_bar_state_v2a_predecessor
BEFORE INSERT ON systematic_fx.campaigns
FOR EACH ROW EXECUTE FUNCTION systematic_fx.enforce_bar_state_v2a_predecessor_campaign();

CREATE OR REPLACE FUNCTION systematic_fx.enforce_bar_state_v2_predecessor_runspec_freeze()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM systematic_fx.campaigns AS campaign
        JOIN systematic_fx.experiments AS experiment
          ON experiment.campaign_id = campaign.campaign_id
        WHERE campaign.campaign_id = NEW.campaign_id
          AND experiment.experiment_id = NEW.experiment_id
          AND campaign.campaign_key = 'bar_state_conditional_v2'
          AND experiment.experiment_key =
              'bar_state_conditional_v2:experiment:frozen_candidate_catalog:v1'
    ) THEN
        PERFORM campaign.campaign_id
        FROM systematic_fx.campaigns AS campaign
        WHERE campaign.campaign_key = 'bar_state_conditional_v2'
        FOR UPDATE;
        IF EXISTS (
            SELECT 1
            FROM systematic_fx.campaigns AS campaign
            WHERE campaign.campaign_key = 'bar_state_conditional_v2a'
        ) THEN
            RAISE EXCEPTION 'State V2 predecessor RunSpec catalog is frozen after V2A registration';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER research_run_specs_freeze_bar_state_v2_predecessor
BEFORE INSERT ON systematic_fx.research_run_specs
FOR EACH ROW EXECUTE FUNCTION systematic_fx.enforce_bar_state_v2_predecessor_runspec_freeze();

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
        JOIN LATERAL systematic_fx.bar_state_governance_profile(
            campaign.campaign_key
        ) AS profile ON profile.experiment_key = experiment.experiment_key
        JOIN systematic_fx.datasets AS dataset
          ON dataset.dataset_id = campaign.dataset_id
        JOIN systematic_fx.research_run_specs AS run_spec
          ON run_spec.research_run_spec_id = target_spec_id
         AND run_spec.campaign_id = campaign.campaign_id
         AND run_spec.experiment_id = experiment.experiment_id
        JOIN systematic_fx.artifacts AS registration_artifact
          ON registration_artifact.artifact_id = experiment.registration_artifact_id
        WHERE trial.experiment_trial_id = target_trial_id
          AND campaign.campaign_key = profile.campaign_key
          AND campaign.name = profile.campaign_name
          AND campaign.status = 'FROZEN'
          AND campaign.frozen_at IS NOT NULL
          AND campaign.holdout_revealed_at IS NULL
          AND campaign.closed_at IS NULL
          AND campaign.config_sha256 = profile.campaign_definition_sha256
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
          AND experiment.experiment_key = profile.experiment_key
          AND experiment.status = 'FROZEN'
          AND experiment.frozen_at IS NOT NULL
          AND experiment.completed_at IS NULL
          AND experiment.primary_family = 'CONDITIONAL_BAR_STATE_MODEL'
          AND experiment.model_family = 'ELASTIC_NET_MULTINOMIAL_LOGISTIC'
          AND experiment.direction = 'BOTH'
          AND experiment.trial_budget = 12
          AND experiment.trials_registered = 12
          AND registration_artifact.artifact_type = profile.artifact_type
          AND registration_artifact.artifact_key =
              profile.artifact_type || ':registration:' ||
              profile.campaign_definition_sha256
          AND registration_artifact.metadata #>> '{artifact_schema}' =
              'systematic_fx.bar_state_registration_artifact.v1'
          AND registration_artifact.metadata #>>
                  '{logical_identity,artifact_kind}' = 'REGISTRATION'
          AND registration_artifact.metadata #>>
                  '{logical_identity,campaign_key}' = profile.campaign_key
          AND registration_artifact.metadata #>>
                  '{logical_identity,campaign_definition_sha256}' =
              profile.campaign_definition_sha256
          AND registration_artifact.metadata #>>
                  '{logical_identity,candidate_catalog_sha256}' =
              profile.candidate_catalog_sha256
          AND registration_artifact.metadata #>>
                  '{logical_identity,lineage,config_file_sha256}' =
              profile.config_file_sha256
          AND registration_artifact.metadata #>>
                  '{logical_identity,lineage,config_semantic_sha256}' =
              profile.config_semantic_sha256
          AND registration_artifact.metadata #>>
                  '{logical_identity,lineage,candidate_catalog_sha256}' =
              profile.candidate_catalog_sha256
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
          AND trial.parameters #>> '{candidate_definition_sha256}' =
              profile.candidate_definition_sha256_by_key ->> trial.trial_key
          AND trial.parameters #>> '{campaign_definition_sha256}' =
              systematic_fx.canonical_jsonb_sha256(
                  trial.parameters -> 'campaign_definition'
              )
          AND trial.parameters #>> '{campaign_definition_sha256}' =
              profile.campaign_definition_sha256
          AND trial.parameters #>> '{candidate_catalog_sha256}' =
              profile.candidate_catalog_sha256
          AND trial.parameters #>> '{config_file_sha256}' =
              profile.config_file_sha256
          AND trial.parameters #>> '{config_semantic_sha256}' =
              profile.config_semantic_sha256
          AND systematic_fx.canonical_jsonb_sha256(
                  trial.parameters #> '{candidate_definition,model_policy}'
              ) = profile.model_policy_sha256
          AND trial.parameters #>>
                  '{candidate_definition,model_policy,sklearn_arguments,max_iter}' =
              profile.model_max_iter::text
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
          AND run_spec.engine_version = profile.engine_version
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

CREATE OR REPLACE FUNCTION systematic_fx.protect_bar_state_campaign_identity()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    governed_old boolean;
    governed_new boolean;
BEGIN
    governed_old := EXISTS (
        SELECT 1
        FROM systematic_fx.bar_state_governance_profile(OLD.campaign_key)
    );
    governed_new := CASE
        WHEN TG_OP = 'DELETE' THEN false
        ELSE EXISTS (
            SELECT 1
            FROM systematic_fx.bar_state_governance_profile(NEW.campaign_key)
        )
    END;
    IF NOT governed_old AND NOT governed_new THEN
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END IF;
    RAISE EXCEPTION 'frozen bar-state campaign identity is immutable';
END;
$$;

CREATE OR REPLACE FUNCTION systematic_fx.protect_bar_state_experiment_identity()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    governed_old boolean;
    governed_new boolean;
BEGIN
    governed_old := systematic_fx.bar_state_experiment_is_governed(
        OLD.experiment_id
    );
    governed_new := CASE
        WHEN TG_OP = 'DELETE' THEN false
        ELSE EXISTS (
            SELECT 1
            FROM systematic_fx.campaigns AS campaign
            JOIN LATERAL systematic_fx.bar_state_governance_profile(
                campaign.campaign_key
            ) AS profile ON profile.experiment_key = NEW.experiment_key
            WHERE campaign.campaign_id = NEW.campaign_id
        )
    END;
    IF NOT governed_old AND NOT governed_new THEN
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END IF;
    RAISE EXCEPTION 'frozen bar-state experiment identity is immutable';
END;
$$;

CREATE OR REPLACE FUNCTION systematic_fx.enforce_bar_state_trial_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    governed_old boolean;
    governed_new boolean;
    mutates_v2 boolean;
BEGIN
    governed_old := CASE WHEN TG_OP = 'INSERT' THEN false
                         ELSE systematic_fx.bar_state_experiment_is_governed(OLD.experiment_id)
                    END;
    governed_new := CASE WHEN TG_OP = 'DELETE' THEN false
                         ELSE systematic_fx.bar_state_experiment_is_governed(NEW.experiment_id)
                    END;
    IF NOT governed_old AND NOT governed_new THEN
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END IF;
    mutates_v2 := CASE
        WHEN TG_OP = 'INSERT' THEN false
        ELSE EXISTS (
            SELECT 1
            FROM systematic_fx.experiments AS experiment
            JOIN systematic_fx.campaigns AS campaign
              ON campaign.campaign_id = experiment.campaign_id
            WHERE experiment.experiment_id = OLD.experiment_id
              AND campaign.campaign_key = 'bar_state_conditional_v2'
        )
    END;
    IF NOT mutates_v2 AND TG_OP <> 'DELETE' THEN
        mutates_v2 := EXISTS (
            SELECT 1
            FROM systematic_fx.experiments AS experiment
            JOIN systematic_fx.campaigns AS campaign
              ON campaign.campaign_id = experiment.campaign_id
            WHERE experiment.experiment_id = NEW.experiment_id
              AND campaign.campaign_key = 'bar_state_conditional_v2'
        );
    END IF;
    IF mutates_v2 THEN
        PERFORM campaign.campaign_id
        FROM systematic_fx.campaigns AS campaign
        WHERE campaign.campaign_key = 'bar_state_conditional_v2'
        FOR UPDATE;
        IF EXISTS (
            SELECT 1
            FROM systematic_fx.campaigns AS campaign
            WHERE campaign.campaign_key = 'bar_state_conditional_v2a'
        ) THEN
            RAISE EXCEPTION 'State V2 predecessor lifecycle is frozen after V2A registration';
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'bar-state candidate trials are append-preserved';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.trial_type <> 'MODEL_FIT'
           OR NEW.status <> 'REGISTERED'
           OR NEW.research_run_spec_id IS NOT NULL
           OR NEW.result_summary <> '{}'::jsonb
           OR NEW.parameters_sha256 IS DISTINCT FROM
                systematic_fx.canonical_jsonb_sha256(NEW.parameters)
           OR NEW.trial_key !~
                '^bsv2_tf(0300|1800)_fs(morphology|state)_cm(005|010|015)$' THEN
            RAISE EXCEPTION 'invalid initial bar-state candidate trial';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.experiment_id IS DISTINCT FROM OLD.experiment_id
       OR NEW.trial_key IS DISTINCT FROM OLD.trial_key
       OR NEW.trial_type IS DISTINCT FROM OLD.trial_type
       OR NEW.parameters IS DISTINCT FROM OLD.parameters
       OR NEW.parameters_sha256 IS DISTINCT FROM OLD.parameters_sha256
       OR NEW.registered_at IS DISTINCT FROM OLD.registered_at THEN
        RAISE EXCEPTION 'bar-state candidate trial identity is immutable';
    END IF;
    IF OLD.status IN ('SUCCEEDED', 'REJECTED', 'FAILED', 'CANCELLED') THEN
        RAISE EXCEPTION 'terminal bar-state candidate trial is immutable';
    END IF;
    IF OLD.research_run_spec_id IS NOT NULL
       AND NEW.research_run_spec_id IS DISTINCT FROM OLD.research_run_spec_id THEN
        RAISE EXCEPTION 'bar-state candidate RunSpec binding is immutable';
    END IF;
    IF NEW.research_run_spec_id IS NOT NULL
       AND NOT systematic_fx.bar_state_run_spec_matches_trial(
           NEW.research_run_spec_id,
           NEW.experiment_trial_id
       ) THEN
        RAISE EXCEPTION 'bar-state candidate requires its exact RunSpec';
    END IF;
    IF NEW.status IS DISTINCT FROM OLD.status
       AND NOT (
           OLD.status IN ('REGISTERED', 'RUNNING')
           AND NEW.status IN ('RUNNING', 'SUCCEEDED', 'REJECTED')
       ) THEN
        RAISE EXCEPTION 'invalid bar-state candidate lifecycle transition';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION systematic_fx.enforce_bar_state_attempt_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    candidate_key text;
    run_fingerprint text;
    profile_campaign_key text;
    profile_version text;
    predecessor_failure_cleanup boolean;
    trial_status text;
    trial_spec_id bigint;
    role_counts jsonb;
    link_manifest_sha256 text;
BEGIN
    SELECT run_spec.canonical_spec #>> '{parameters,bar_state_candidate_key}',
           run_spec.run_fingerprint, profile.campaign_key,
           profile.profile_version
    INTO candidate_key, run_fingerprint, profile_campaign_key, profile_version
    FROM systematic_fx.research_run_specs AS run_spec
    JOIN systematic_fx.campaigns AS campaign
      ON campaign.campaign_id = run_spec.campaign_id
    JOIN LATERAL systematic_fx.bar_state_governance_profile(
        campaign.campaign_key
    ) AS profile ON true
    WHERE run_spec.research_run_spec_id = NEW.research_run_spec_id
    ;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;
    IF profile_version = 'V2' THEN
        PERFORM campaign.campaign_id
        FROM systematic_fx.campaigns AS campaign
        WHERE campaign.campaign_key = profile_campaign_key
        FOR UPDATE;
        IF EXISTS (
            SELECT 1
            FROM systematic_fx.campaigns AS campaign
            WHERE campaign.campaign_key = 'bar_state_conditional_v2a'
        ) THEN
            RAISE EXCEPTION 'State V2 predecessor lifecycle is frozen after V2A registration';
        END IF;
    END IF;
    predecessor_failure_cleanup := CASE
        WHEN TG_OP = 'UPDATE' THEN
            profile_version = 'V2A'
            AND OLD.status IN ('QUEUED', 'RUNNING')
            AND NEW.status = 'FAILED'
        ELSE false
    END;
    SELECT trial.status, trial.research_run_spec_id
    INTO trial_status, trial_spec_id
    FROM systematic_fx.experiment_trials AS trial
    JOIN systematic_fx.research_run_specs AS run_spec
      ON run_spec.experiment_id = trial.experiment_id
    WHERE run_spec.research_run_spec_id = NEW.research_run_spec_id
      AND trial.trial_key = candidate_key;
    IF trial_spec_id IS DISTINCT FROM NEW.research_run_spec_id
       OR (
            NOT predecessor_failure_cleanup
            AND NOT systematic_fx.bar_state_catalog_preregistered(
                NEW.research_run_spec_id
            )
       ) THEN
        RAISE EXCEPTION 'bar-state attempt requires all 12 exact prebound candidates';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.status NOT IN ('QUEUED', 'SKIPPED_DUPLICATE') THEN
            RAISE EXCEPTION 'bar-state attempts must begin QUEUED or duplicate';
        END IF;
    ELSIF NOT (
        (OLD.status = 'QUEUED' AND NEW.status IN ('RUNNING', 'FAILED'))
        OR (OLD.status = 'RUNNING' AND NEW.status IN ('SUCCEEDED', 'FAILED'))
    ) THEN
        RAISE EXCEPTION 'invalid bar-state attempt transition';
    END IF;
    IF NEW.status IN ('QUEUED', 'RUNNING') THEN
        IF trial_status NOT IN ('REGISTERED', 'RUNNING')
           OR NEW.result_artifact_id IS NOT NULL
           OR NEW.trade_ledger_artifact_id IS NOT NULL
           OR NEW.reused_attempt_id IS NOT NULL
           OR NEW.result_summary <> '{}'::jsonb
           OR NEW.error_message IS NOT NULL
           OR NEW.finished_at IS NOT NULL THEN
            RAISE EXCEPTION 'active bar-state attempt has invalid state';
        END IF;
    ELSIF NEW.status = 'FAILED' THEN
        IF trial_status NOT IN ('REGISTERED', 'RUNNING')
           OR NEW.result_artifact_id IS NOT NULL
           OR NEW.trade_ledger_artifact_id IS NOT NULL
           OR NEW.reused_attempt_id IS NOT NULL
           OR NEW.result_summary #>> '{candidate_key}' IS DISTINCT FROM candidate_key
           OR NEW.result_summary #>> '{run_fingerprint}' IS DISTINCT FROM run_fingerprint
           OR btrim(COALESCE(NEW.error_message, '')) = ''
           OR NEW.finished_at IS NULL THEN
            RAISE EXCEPTION 'failed bar-state attempt has invalid lineage';
        END IF;
    ELSIF NEW.status = 'SUCCEEDED' THEN
        SELECT jsonb_object_agg(artifact_role, role_count)
        INTO role_counts
        FROM (
            SELECT artifact_role, count(*) AS role_count
            FROM systematic_fx.bar_state_artifact_links
            WHERE research_run_attempt_id = NEW.research_run_attempt_id
            GROUP BY artifact_role
        ) AS counts;
        SELECT systematic_fx.canonical_jsonb_sha256(
            jsonb_agg(
                jsonb_build_object(
                    'artifact_id', artifact_id,
                    'artifact_identity_sha256', artifact_identity_sha256,
                    'artifact_role', artifact_role,
                    'content_sha256', content_sha256,
                    'lineage_sha256', lineage_sha256,
                    'shard_ordinal', shard_ordinal,
                    'split_key', split_key
                ) ORDER BY artifact_role, split_key, shard_ordinal
            )
        )
        INTO link_manifest_sha256
        FROM systematic_fx.bar_state_artifact_links
        WHERE research_run_attempt_id = NEW.research_run_attempt_id;
        IF trial_status NOT IN ('REGISTERED', 'RUNNING', 'SUCCEEDED', 'REJECTED')
           OR NEW.result_artifact_id IS NULL
           OR NEW.trade_ledger_artifact_id IS NOT NULL
           OR NEW.reused_attempt_id IS NOT NULL
           OR NEW.error_message IS NOT NULL
           OR NEW.finished_at IS NULL
           OR NEW.result_summary #>> '{schema}' IS DISTINCT FROM
                'systematic_fx.bar_state_terminal_summary.v1'
           OR NEW.result_summary #>> '{candidate_key}' IS DISTINCT FROM candidate_key
           OR NEW.result_summary #>> '{run_fingerprint}' IS DISTINCT FROM run_fingerprint
           OR NEW.result_summary #>> '{attempt_status}' IS DISTINCT FROM 'SUCCEEDED'
           OR NEW.result_summary #>> '{result_artifact_id}' IS DISTINCT FROM
                NEW.result_artifact_id::text
           OR NOT systematic_fx.jsonb_has_exact_keys(
                NEW.result_summary,
                ARRAY[
                    'artifact_link_manifest_sha256', 'artifact_role_counts',
                    'attempt_status', 'candidate_key',
                    'candidate_evidence_slice_sha256',
                    'candidate_selection_projection_sha256',
                    'candidate_selection_sha256', 'compact_summary',
                    'decision_label', 'finalist_model_binding_sha256',
                    'global_evidence_projection_sha256',
                    'model_package_projection_sha256',
                    'result_artifact_id', 'run_fingerprint', 'schema', 'trial_status'
                ]
              )
           OR NEW.result_summary #>> '{candidate_selection_sha256}'
                !~ '^[0-9a-f]{64}$'
           OR NEW.result_summary #>> '{candidate_evidence_slice_sha256}'
                !~ '^[0-9a-f]{64}$'
           OR NEW.result_summary #>> '{candidate_selection_projection_sha256}'
                !~ '^[0-9a-f]{64}$'
           OR NEW.result_summary #>> '{finalist_model_binding_sha256}'
                !~ '^[0-9a-f]{64}$'
           OR NEW.result_summary #>> '{global_evidence_projection_sha256}'
                !~ '^[0-9a-f]{64}$'
           OR NEW.result_summary #>> '{model_package_projection_sha256}'
                !~ '^[0-9a-f]{64}$'
           OR jsonb_typeof(NEW.result_summary -> 'compact_summary')
                IS DISTINCT FROM 'object'
           OR NOT systematic_fx.jsonb_has_exact_keys(
                NEW.result_summary -> 'compact_summary',
                ARRAY[
                    'candidate_key', 'discovery_final_fit_model_sha256',
                    'final_label', 'positive_component_size', 'price_policy',
                    'rejection_reasons', 'selected_stop_loss_index',
                    'selected_take_profit_index'
                ]
              )
           OR NEW.result_summary #>> '{compact_summary,candidate_key}'
                IS DISTINCT FROM candidate_key
           OR systematic_fx.canonical_jsonb_sha256(
                systematic_fx.bar_state_candidate_selection_projection(
                    NEW.result_summary -> 'compact_summary'
                )
              ) IS DISTINCT FROM
                NEW.result_summary #>> '{candidate_selection_projection_sha256}'
           OR (
                NEW.result_summary #>> '{compact_summary,final_label}'
                    IS DISTINCT FROM 'FINALIST'
                AND NEW.result_summary #>> '{compact_summary,final_label}'
                    IS DISTINCT FROM 'REJECTED'
              )
           OR (
                (NEW.result_summary #>> '{trial_status}' = 'SUCCEEDED')
                    IS DISTINCT FROM
                (NEW.result_summary #>> '{compact_summary,final_label}' = 'FINALIST')
              )
           OR (
                NEW.result_summary #>> '{compact_summary,final_label}' = 'FINALIST'
                AND (
                    NEW.result_summary #> '{compact_summary,rejection_reasons}'
                        IS DISTINCT FROM '[]'::jsonb
                    OR jsonb_typeof(
                        NEW.result_summary #> '{compact_summary,positive_component_size}'
                    ) IS DISTINCT FROM 'number'
                    OR NEW.result_summary #>> '{compact_summary,positive_component_size}'
                        !~ '^[0-9]+$'
                    OR (NEW.result_summary #>>
                        '{compact_summary,positive_component_size}')::integer < 9
                    OR
                    systematic_fx.bar_state_economic_multiplier(
                        NEW.result_summary #>
                            '{compact_summary,selected_stop_loss_index}'
                    ) IS NULL
                    OR systematic_fx.bar_state_economic_multiplier(
                        NEW.result_summary #>
                            '{compact_summary,selected_take_profit_index}'
                    ) IS NULL
                )
              )
           OR (
                NEW.result_summary #>> '{compact_summary,final_label}' = 'REJECTED'
                AND NOT (
                    (
                        NEW.result_summary #>
                            '{compact_summary,selected_stop_loss_index}' = 'null'::jsonb
                        AND NEW.result_summary #>
                            '{compact_summary,selected_take_profit_index}' = 'null'::jsonb
                        AND NEW.result_summary #>
                            '{compact_summary,rejection_reasons}' <>
                            '["MAXIMUM_FINALIST_LIMIT"]'::jsonb
                        AND jsonb_typeof(
                            NEW.result_summary #> '{compact_summary,rejection_reasons}'
                        ) = 'array'
                        AND jsonb_array_length(
                            NEW.result_summary #> '{compact_summary,rejection_reasons}'
                        ) > 0
                        AND NEW.result_summary #>
                            '{compact_summary,positive_component_size}' = '0'::jsonb
                    ) OR (
                        systematic_fx.bar_state_economic_multiplier(
                            NEW.result_summary #>
                                '{compact_summary,selected_stop_loss_index}'
                        ) IS NOT NULL
                        AND systematic_fx.bar_state_economic_multiplier(
                            NEW.result_summary #>
                                '{compact_summary,selected_take_profit_index}'
                        ) IS NOT NULL
                        AND NEW.result_summary #>
                            '{compact_summary,rejection_reasons}' =
                            '["MAXIMUM_FINALIST_LIMIT"]'::jsonb
                        AND jsonb_typeof(
                            NEW.result_summary #> '{compact_summary,positive_component_size}'
                        ) = 'number'
                        AND NEW.result_summary #>>
                            '{compact_summary,positive_component_size}' ~ '^[0-9]+$'
                        AND (NEW.result_summary #>>
                            '{compact_summary,positive_component_size}')::integer >= 9
                    )
                )
              )
           OR NEW.result_summary #> '{compact_summary,price_policy}'
                IS DISTINCT FROM jsonb_build_object(
                    'entry_reference',
                        'NEXT_SIGNAL_BAR_FIRST_TRADE_PLUS_SCENARIO_ADVERSITY',
                    'long', jsonb_build_object(
                        'buying_price', 'ENTRY_FILL_PRICE',
                        'loss_price',
                            'ENTRY_FILL_PRICE_MINUS_REALIZED_STOP_LOSS_TICKS',
                        'sell_price',
                            'ENTRY_FILL_PRICE_PLUS_REALIZED_TAKE_PROFIT_TICKS'
                    ),
                    'selected_stop_loss_multiplier',
                        systematic_fx.bar_state_economic_multiplier(
                            NEW.result_summary #>
                                '{compact_summary,selected_stop_loss_index}'
                        ),
                    'selected_take_profit_multiplier',
                        systematic_fx.bar_state_economic_multiplier(
                            NEW.result_summary #>
                                '{compact_summary,selected_take_profit_index}'
                        ),
                    'short', jsonb_build_object(
                        'buying_price',
                            'ENTRY_FILL_PRICE_MINUS_REALIZED_TAKE_PROFIT_TICKS',
                        'loss_price',
                            'ENTRY_FILL_PRICE_PLUS_REALIZED_STOP_LOSS_TICKS',
                        'sell_price', 'ENTRY_FILL_PRICE'
                    ),
                    'trade_level_exact_prices_artifact_role', 'OOS_TRADE'
                )
           OR (
                NEW.result_summary #>> '{trial_status}' = 'SUCCEEDED'
                AND NEW.result_summary #>>
                    '{compact_summary,discovery_final_fit_model_sha256}'
                    !~ '^[0-9a-f]{64}$'
              )
           OR (
                NEW.result_summary #>> '{trial_status}' = 'REJECTED'
                AND NEW.result_summary #>
                    '{compact_summary,discovery_final_fit_model_sha256}'
                    IS DISTINCT FROM 'null'::jsonb
              )
           OR NEW.result_summary -> 'artifact_role_counts' IS DISTINCT FROM role_counts
           OR NEW.result_summary #>> '{artifact_link_manifest_sha256}'
                IS DISTINCT FROM link_manifest_sha256
           OR role_counts IS DISTINCT FROM jsonb_build_object(
                'FEATURE', 4,
                'GLOBAL_RESULT', 1,
                'LABEL', 4,
                'MODEL', 1,
                'OOS_TRADE', 1,
                'TERMINAL_RESULT', 1
              )
           OR EXISTS (
                SELECT 1
                FROM systematic_fx.bar_state_artifact_links AS link
                WHERE link.research_run_attempt_id = NEW.research_run_attempt_id
                  AND link.split_key IS DISTINCT FROM 'discovery'
           )
           OR (
                SELECT array_agg(link.shard_ordinal ORDER BY link.shard_ordinal)
                FROM systematic_fx.bar_state_artifact_links AS link
                WHERE link.research_run_attempt_id = NEW.research_run_attempt_id
                  AND link.artifact_role = 'FEATURE'
              ) IS DISTINCT FROM ARRAY[0, 1, 2, 3]
           OR (
                SELECT array_agg(link.shard_ordinal ORDER BY link.shard_ordinal)
                FROM systematic_fx.bar_state_artifact_links AS link
                WHERE link.research_run_attempt_id = NEW.research_run_attempt_id
                  AND link.artifact_role = 'LABEL'
              ) IS DISTINCT FROM ARRAY[0, 1, 2, 3]
           OR (
                SELECT array_agg(link.shard_ordinal ORDER BY link.shard_ordinal)
                FROM systematic_fx.bar_state_artifact_links AS link
                WHERE link.research_run_attempt_id = NEW.research_run_attempt_id
                  AND link.artifact_role = 'OOS_TRADE'
              ) IS DISTINCT FROM ARRAY[0]
           OR (
                SELECT array_agg(link.shard_ordinal ORDER BY link.shard_ordinal)
                FROM systematic_fx.bar_state_artifact_links AS link
                WHERE link.research_run_attempt_id = NEW.research_run_attempt_id
                  AND link.artifact_role = 'MODEL'
              ) IS DISTINCT FROM ARRAY[0]
           OR (
                SELECT array_agg(link.shard_ordinal ORDER BY link.shard_ordinal)
                FROM systematic_fx.bar_state_artifact_links AS link
                WHERE link.research_run_attempt_id = NEW.research_run_attempt_id
                  AND link.artifact_role = 'GLOBAL_RESULT'
              ) IS DISTINCT FROM ARRAY[0]
           OR (
                SELECT array_agg(link.shard_ordinal ORDER BY link.shard_ordinal)
                FROM systematic_fx.bar_state_artifact_links AS link
                WHERE link.research_run_attempt_id = NEW.research_run_attempt_id
                  AND link.artifact_role = 'TERMINAL_RESULT'
              ) IS DISTINCT FROM ARRAY[0]
           OR (
                (
                    NEW.result_summary #>> '{decision_label}'
                        IS DISTINCT FROM 'DISCOVERY_FINALIST'
                    OR NEW.result_summary #>> '{trial_status}'
                        IS DISTINCT FROM 'SUCCEEDED'
                ) AND (
                    NEW.result_summary #>> '{decision_label}'
                        IS DISTINCT FROM 'DISCOVERY_REJECT'
                    OR NEW.result_summary #>> '{trial_status}'
                        IS DISTINCT FROM 'REJECTED'
                )
           )
           OR NOT EXISTS (
                SELECT 1
                FROM systematic_fx.bar_state_artifact_links AS link
                JOIN systematic_fx.artifacts AS terminal_artifact
                  ON terminal_artifact.artifact_id = link.artifact_id
                JOIN systematic_fx.bar_state_artifact_links AS global_link
                  ON global_link.research_run_attempt_id =
                     link.research_run_attempt_id
                 AND global_link.artifact_role = 'GLOBAL_RESULT'
                 AND global_link.split_key = 'discovery'
                 AND global_link.shard_ordinal = 0
                JOIN systematic_fx.artifacts AS global_artifact
                  ON global_artifact.artifact_id = global_link.artifact_id
                JOIN systematic_fx.bar_state_artifact_links AS model_link
                  ON model_link.research_run_attempt_id = link.research_run_attempt_id
                 AND model_link.artifact_role = 'MODEL'
                 AND model_link.split_key = 'discovery'
                 AND model_link.shard_ordinal = 0
                JOIN systematic_fx.artifacts AS model_artifact
                  ON model_artifact.artifact_id = model_link.artifact_id
                JOIN systematic_fx.bar_state_artifact_links AS oos_link
                  ON oos_link.research_run_attempt_id = link.research_run_attempt_id
                 AND oos_link.artifact_role = 'OOS_TRADE'
                 AND oos_link.split_key = 'discovery'
                 AND oos_link.shard_ordinal = 0
                JOIN systematic_fx.artifacts AS oos_artifact
                  ON oos_artifact.artifact_id = oos_link.artifact_id
                WHERE link.research_run_attempt_id = NEW.research_run_attempt_id
                  AND link.artifact_role = 'TERMINAL_RESULT'
                  AND link.artifact_id = NEW.result_artifact_id
                  AND terminal_artifact.metadata #>>
                        '{logical_identity,candidate_key}' IS NOT DISTINCT FROM
                        candidate_key
                  AND terminal_artifact.metadata #>>
                        '{logical_identity,decision_label}' IS NOT DISTINCT FROM
                        NEW.result_summary #>> '{decision_label}'
                  AND terminal_artifact.metadata #>>
                        '{logical_identity,trial_status}' IS NOT DISTINCT FROM
                        NEW.result_summary #>> '{trial_status}'
                  AND terminal_artifact.metadata #>>
                        '{logical_identity,compact_summary_sha256}'
                        IS NOT DISTINCT FROM systematic_fx.canonical_jsonb_sha256(
                            NEW.result_summary -> 'compact_summary'
                        )
                  AND terminal_artifact.metadata #>>
                        '{logical_identity,candidate_evidence_slice_sha256}'
                        IS NOT DISTINCT FROM NEW.result_summary #>>
                            '{candidate_evidence_slice_sha256}'
                  AND terminal_artifact.metadata #>>
                        '{logical_identity,candidate_selection_sha256}'
                        IS NOT DISTINCT FROM NEW.result_summary #>>
                            '{candidate_selection_sha256}'
                  AND terminal_artifact.metadata #>>
                        '{logical_identity,candidate_selection_projection_sha256}'
                        IS NOT DISTINCT FROM NEW.result_summary #>>
                            '{candidate_selection_projection_sha256}'
                  AND terminal_artifact.metadata #>>
                        '{logical_identity,global_evidence_projection_sha256}'
                        IS NOT DISTINCT FROM NEW.result_summary #>>
                            '{global_evidence_projection_sha256}'
                  AND terminal_artifact.metadata #>>
                        '{logical_identity,model_package_projection_sha256}'
                        IS NOT DISTINCT FROM NEW.result_summary #>>
                            '{model_package_projection_sha256}'
                  AND terminal_artifact.metadata #>>
                        '{logical_identity,finalist_model_binding_sha256}'
                        IS NOT DISTINCT FROM NEW.result_summary #>>
                            '{finalist_model_binding_sha256}'
                  AND model_artifact.metadata #>>
                        '{logical_identity,candidate_selection_sha256}'
                        IS NOT DISTINCT FROM NEW.result_summary #>>
                            '{candidate_selection_sha256}'
                  AND model_artifact.metadata #>>
                        '{logical_identity,candidate_selection_projection_sha256}'
                        IS NOT DISTINCT FROM NEW.result_summary #>>
                            '{candidate_selection_projection_sha256}'
                  AND model_artifact.metadata #>>
                        '{logical_identity,global_evidence_projection_sha256}'
                        IS NOT DISTINCT FROM NEW.result_summary #>>
                            '{global_evidence_projection_sha256}'
                  AND model_artifact.metadata #>>
                        '{logical_identity,model_package_projection_sha256}'
                        IS NOT DISTINCT FROM NEW.result_summary #>>
                            '{model_package_projection_sha256}'
                  AND model_artifact.metadata #>
                        '{logical_identity,finalist_model_binding}'
                        IS NOT DISTINCT FROM terminal_artifact.metadata #>
                            '{logical_identity,finalist_model_binding}'
                  AND model_artifact.metadata #>>
                        '{logical_identity,finalist_model_binding_sha256}'
                        IS NOT DISTINCT FROM NEW.result_summary #>>
                            '{finalist_model_binding_sha256}'
                  AND global_artifact.metadata #>
                        '{logical_identity,candidate_evidence_slice_sha256_by_key}'
                        ->> candidate_key IS NOT DISTINCT FROM
                      NEW.result_summary #>> '{candidate_evidence_slice_sha256}'
                  AND global_artifact.metadata #>
                        '{logical_identity,candidate_selection_sha256_by_key}'
                        ->> candidate_key IS NOT DISTINCT FROM
                      NEW.result_summary #>> '{candidate_selection_sha256}'
                  AND global_artifact.metadata #>
                        '{logical_identity,candidate_selection_projection_sha256_by_key}'
                        ->> candidate_key IS NOT DISTINCT FROM
                      NEW.result_summary #>> '{candidate_selection_projection_sha256}'
                  AND global_artifact.metadata #>>
                        '{logical_identity,global_evidence_projection_sha256}'
                        IS NOT DISTINCT FROM NEW.result_summary #>>
                            '{global_evidence_projection_sha256}'
                  AND global_artifact.metadata #>
                        '{logical_identity,model_package_projection_sha256_by_key}'
                        ->> candidate_key IS NOT DISTINCT FROM
                      NEW.result_summary #>> '{model_package_projection_sha256}'
                  AND global_artifact.metadata #>
                        '{logical_identity,finalist_model_binding_by_key}'
                        -> candidate_key IS NOT DISTINCT FROM
                      terminal_artifact.metadata #>
                        '{logical_identity,finalist_model_binding}'
                  AND global_artifact.metadata #>
                        '{logical_identity,finalist_model_binding_sha256_by_key}'
                        ->> candidate_key IS NOT DISTINCT FROM
                      NEW.result_summary #>> '{finalist_model_binding_sha256}'
                  AND global_artifact.metadata #>
                        '{logical_identity,candidate_oos_trade_record_count_by_key}'
                        ->> candidate_key IS NOT DISTINCT FROM
                      oos_artifact.metadata #>> '{record_count}'
                  AND oos_artifact.metadata #>> '{logical_identity,row_count}'
                        IS NOT DISTINCT FROM oos_artifact.metadata #>> '{record_count}'
                  AND (
                      (
                          NEW.result_summary #>> '{trial_status}' = 'SUCCEEDED'
                          AND terminal_artifact.metadata #>>
                                '{logical_identity,finalist_model_binding,model_sha256}'
                              = NEW.result_summary #>>
                                '{compact_summary,discovery_final_fit_model_sha256}'
                      ) OR (
                          NEW.result_summary #>> '{trial_status}' = 'REJECTED'
                          AND terminal_artifact.metadata #>
                                '{logical_identity,finalist_model_binding}' = 'null'::jsonb
                      )
                  )
           ) THEN
            RAISE EXCEPTION 'succeeded bar-state attempt lacks complete exact evidence';
        END IF;
    ELSIF NEW.status = 'SKIPPED_DUPLICATE' THEN
        IF trial_status NOT IN ('SUCCEEDED', 'REJECTED')
           OR NEW.result_artifact_id IS NOT NULL
           OR NEW.trade_ledger_artifact_id IS NOT NULL
           OR NEW.started_at IS NOT NULL
           OR NEW.finished_at IS NULL
           OR NEW.error_message IS NOT NULL
           OR NEW.result_summary IS DISTINCT FROM jsonb_build_object(
                'reason', 'EXACT_FINGERPRINT_ALREADY_SUCCEEDED',
                'reused_attempt_id', NEW.reused_attempt_id
           )
           OR NOT EXISTS (
                SELECT 1
                FROM systematic_fx.research_run_attempts AS source
                WHERE source.research_run_attempt_id = NEW.reused_attempt_id
                  AND source.research_run_spec_id = NEW.research_run_spec_id
                  AND source.status = 'SUCCEEDED'
           ) THEN
            RAISE EXCEPTION 'duplicate bar-state attempt has invalid source';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION systematic_fx.enforce_bar_state_artifact_link()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    candidate_key text;
    run_fingerprint text;
    expected_schema text;
    attempt_status text;
    trial_spec_id bigint;
    trial_id bigint;
    spec_campaign_id bigint;
    expected_candidate_definition_sha256 text;
    expected_code_snapshot_sha256 text;
    expected_dependency_lock_sha256 text;
    expected_runtime_environment_sha256 text;
    expected_ordered_run_set_sha256 text;
    registered_ordered_run_set_sha256 text;
    profile_campaign_key text;
    profile_artifact_type text;
    profile_config_file_sha256 text;
    profile_config_semantic_sha256 text;
    profile_candidate_catalog_sha256 text;
    artifact_record systematic_fx.artifacts%ROWTYPE;
BEGIN
    SELECT attempt.status, run_spec.run_fingerprint, run_spec.campaign_id,
           run_spec.canonical_spec #>> '{parameters,bar_state_candidate_key}',
           trial.experiment_trial_id, trial.research_run_spec_id,
           trial.parameters #>> '{candidate_definition_sha256}',
           run_spec.code_snapshot_sha256, run_spec.dependency_lock_sha256,
           systematic_fx.canonical_jsonb_sha256(
               run_spec.canonical_spec -> 'runtime_environment'
           ),
           (
               SELECT systematic_fx.canonical_jsonb_sha256(
                   jsonb_agg(
                       catalog_spec.run_fingerprint
                       ORDER BY catalog_trial.trial_key
                   )
               )
               FROM systematic_fx.experiment_trials AS catalog_trial
               JOIN systematic_fx.research_run_specs AS catalog_spec
                 ON catalog_spec.research_run_spec_id =
                    catalog_trial.research_run_spec_id
               WHERE catalog_trial.experiment_id = run_spec.experiment_id
           ),
           registration_artifact.metadata #>>
               '{logical_identity,lineage,ordered_run_set_sha256}',
           profile.campaign_key, profile.artifact_type,
           profile.config_file_sha256, profile.config_semantic_sha256,
           profile.candidate_catalog_sha256
    INTO attempt_status, run_fingerprint, spec_campaign_id, candidate_key,
         trial_id, trial_spec_id, expected_candidate_definition_sha256,
         expected_code_snapshot_sha256, expected_dependency_lock_sha256,
         expected_runtime_environment_sha256, expected_ordered_run_set_sha256,
         registered_ordered_run_set_sha256, profile_campaign_key,
         profile_artifact_type, profile_config_file_sha256,
         profile_config_semantic_sha256, profile_candidate_catalog_sha256
    FROM systematic_fx.research_run_attempts AS attempt
    JOIN systematic_fx.research_run_specs AS run_spec
      ON run_spec.research_run_spec_id = attempt.research_run_spec_id
    JOIN systematic_fx.experiment_trials AS trial
      ON trial.experiment_id = run_spec.experiment_id
     AND trial.trial_key =
         run_spec.canonical_spec #>> '{parameters,bar_state_candidate_key}'
    JOIN systematic_fx.experiments AS experiment
      ON experiment.experiment_id = run_spec.experiment_id
    JOIN systematic_fx.artifacts AS registration_artifact
      ON registration_artifact.artifact_id = experiment.registration_artifact_id
    JOIN systematic_fx.campaigns AS campaign
      ON campaign.campaign_id = run_spec.campaign_id
    JOIN systematic_fx.datasets AS dataset
      ON dataset.dataset_id = campaign.dataset_id
    JOIN LATERAL systematic_fx.bar_state_governance_profile(
        campaign.campaign_key
    ) AS profile ON profile.experiment_key = experiment.experiment_key
    WHERE attempt.research_run_attempt_id = NEW.research_run_attempt_id
      AND run_spec.research_run_spec_id = NEW.research_run_spec_id
      AND dataset.dataset_key = 'glbx_mdp3_mbp_10_6e_fut_v1'
      AND dataset.manifest_sha256 =
          '14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de'
      AND dataset.status NOT IN ('REJECTED', 'RETIRED')
    ;
    IF NOT FOUND
       OR attempt_status IS DISTINCT FROM 'RUNNING'
       OR spec_campaign_id IS DISTINCT FROM NEW.campaign_id
       OR trial_id IS DISTINCT FROM NEW.experiment_trial_id
       OR trial_spec_id IS DISTINCT FROM NEW.research_run_spec_id
       OR expected_candidate_definition_sha256 IS NULL
       OR expected_code_snapshot_sha256 IS NULL
       OR expected_dependency_lock_sha256 IS NULL
       OR expected_runtime_environment_sha256 IS NULL
       OR expected_ordered_run_set_sha256 IS NULL
       OR registered_ordered_run_set_sha256 IS DISTINCT FROM
            expected_ordered_run_set_sha256 THEN
        RAISE EXCEPTION 'bar-state artifact link requires its RUNNING exact candidate';
    END IF;
    SELECT * INTO artifact_record
    FROM systematic_fx.artifacts
    WHERE artifact_id = NEW.artifact_id;
    expected_schema := CASE NEW.artifact_role
        WHEN 'FEATURE' THEN 'systematic_fx.bar_state_feature_artifact.v1'
        WHEN 'LABEL' THEN 'systematic_fx.bar_state_label_artifact.v1'
        WHEN 'MODEL' THEN 'systematic_fx.bar_state_model_artifact.v1'
        WHEN 'OOS_TRADE' THEN 'systematic_fx.bar_state_oos_trade_artifact.v1'
        WHEN 'GLOBAL_RESULT' THEN 'systematic_fx.bar_state_global_result_artifact.v1'
        WHEN 'TERMINAL_RESULT' THEN 'systematic_fx.bar_state_terminal_result_artifact.v1'
    END;
    IF artifact_record.artifact_id IS NULL
       OR artifact_record.artifact_type IS DISTINCT FROM profile_artifact_type
       OR artifact_record.artifact_key NOT LIKE
            profile_artifact_type || ':' || lower(NEW.artifact_role) || ':%'
       OR artifact_record.sha256 IS DISTINCT FROM NEW.content_sha256
       OR artifact_record.metadata #>> '{content_sha256}' IS DISTINCT FROM
            NEW.content_sha256
       OR artifact_record.metadata #>> '{artifact_identity_sha256}' IS DISTINCT FROM
            NEW.artifact_identity_sha256
       OR artifact_record.metadata #>> '{artifact_schema}' IS DISTINCT FROM
            expected_schema
       OR artifact_record.metadata #>> '{root_kind}' IS DISTINCT FROM 'bar_patterns'
       OR artifact_record.metadata #>> '{identity_schema}' IS DISTINCT FROM
            'systematic_fx.bar_artifact_identity.v1'
       OR artifact_record.metadata #>> '{logical_identity,artifact_kind}' IS DISTINCT FROM
            NEW.artifact_role
       OR artifact_record.metadata #>> '{logical_identity,campaign_key}' IS DISTINCT FROM
            profile_campaign_key
       OR NOT systematic_fx.jsonb_has_exact_keys(
            artifact_record.metadata #> '{logical_identity,lineage}',
            ARRAY[
                'bar_dataset_manifest_sha256', 'candidate_catalog_sha256',
                'candidate_definition_sha256', 'candidate_key',
                'code_snapshot_sha256', 'config_file_sha256',
                'config_semantic_sha256', 'dependency_lock_sha256',
                'discovery_scope', 'discovery_scope_sha256',
                'ordered_run_set_sha256', 'parent_artifacts',
                'raw_source_manifest_sha256', 'run_fingerprint',
                'runtime_environment_sha256', 'schema',
                'training_plan_sha256'
            ]
          )
       OR artifact_record.metadata #>>
            '{logical_identity,lineage,schema}' IS DISTINCT FROM
            'systematic_fx.bar_state_artifact_lineage.v1'
       OR artifact_record.metadata #>>
            '{logical_identity,lineage,bar_dataset_manifest_sha256}' IS DISTINCT FROM
            'e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc'
       OR artifact_record.metadata #>>
            '{logical_identity,lineage,raw_source_manifest_sha256}' IS DISTINCT FROM
            '14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de'
       OR artifact_record.metadata #>>
            '{logical_identity,lineage,config_file_sha256}' IS DISTINCT FROM
            profile_config_file_sha256
       OR artifact_record.metadata #>>
            '{logical_identity,lineage,config_semantic_sha256}' IS DISTINCT FROM
            profile_config_semantic_sha256
       OR artifact_record.metadata #>>
            '{logical_identity,lineage,candidate_catalog_sha256}' IS DISTINCT FROM
            profile_candidate_catalog_sha256
       OR artifact_record.metadata #>>
            '{logical_identity,lineage,training_plan_sha256}' IS DISTINCT FROM
            '9a4833aa53fe03788ddf224efcd24abbcc492498b915f174b6473f9c37f3fc8a'
       OR artifact_record.metadata #>>
            '{logical_identity,lineage,code_snapshot_sha256}' IS DISTINCT FROM
            expected_code_snapshot_sha256
       OR artifact_record.metadata #>>
            '{logical_identity,lineage,dependency_lock_sha256}' IS DISTINCT FROM
            expected_dependency_lock_sha256
       OR artifact_record.metadata #>>
            '{logical_identity,lineage,runtime_environment_sha256}' IS DISTINCT FROM
            expected_runtime_environment_sha256
       OR artifact_record.metadata #>>
            '{logical_identity,lineage,ordered_run_set_sha256}' IS DISTINCT FROM
            expected_ordered_run_set_sha256
       OR artifact_record.metadata #>>
            '{logical_identity,lineage,discovery_scope_sha256}' IS DISTINCT FROM
            '35e59a3475d9e79e17e1b132b6a2044458f46069a715ceb7c458ad298cab3ec0'
       OR systematic_fx.canonical_jsonb_sha256(
            artifact_record.metadata #> '{logical_identity,lineage,discovery_scope}'
          ) IS DISTINCT FROM
            '35e59a3475d9e79e17e1b132b6a2044458f46069a715ceb7c458ad298cab3ec0'
       OR jsonb_typeof(
            artifact_record.metadata #> '{logical_identity,lineage,parent_artifacts}'
          ) IS DISTINCT FROM 'array'
       OR artifact_record.metadata #>>
            '{logical_identity,lineage_sha256}' IS DISTINCT FROM
            NEW.lineage_sha256
       OR systematic_fx.canonical_jsonb_sha256(
            artifact_record.metadata #> '{logical_identity,lineage}'
          ) IS DISTINCT FROM NEW.lineage_sha256
       OR systematic_fx.canonical_jsonb_sha256(
            artifact_record.metadata - 'artifact_identity_sha256' - 'content_sha256'
          ) IS DISTINCT FROM NEW.artifact_identity_sha256 THEN
        RAISE EXCEPTION 'bar-state artifact bytes or lineage drifted';
    END IF;
    IF NEW.artifact_role IN ('MODEL', 'OOS_TRADE', 'TERMINAL_RESULT')
       AND (
           artifact_record.metadata #>>
                '{logical_identity,candidate_key}' IS DISTINCT FROM candidate_key
           OR artifact_record.metadata #>>
                '{logical_identity,lineage,candidate_definition_sha256}'
                IS DISTINCT FROM expected_candidate_definition_sha256
           OR artifact_record.metadata #>>
                '{logical_identity,lineage,candidate_key}' IS DISTINCT FROM candidate_key
           OR artifact_record.metadata #>>
                '{logical_identity,lineage,run_fingerprint}' IS DISTINCT FROM
                run_fingerprint
       ) THEN
        RAISE EXCEPTION 'candidate-specific bar-state artifact lineage drifted';
    END IF;
    IF NEW.artifact_role IN ('FEATURE', 'LABEL', 'GLOBAL_RESULT')
       AND (
           artifact_record.metadata #>
                '{logical_identity,lineage,candidate_definition_sha256}'
                IS DISTINCT FROM 'null'::jsonb
           OR artifact_record.metadata #> '{logical_identity,lineage,candidate_key}'
                IS DISTINCT FROM 'null'::jsonb
           OR artifact_record.metadata #> '{logical_identity,lineage,run_fingerprint}'
                IS DISTINCT FROM 'null'::jsonb
       ) THEN
        RAISE EXCEPTION 'shared bar-state artifact claims candidate-specific lineage';
    END IF;
    IF NEW.artifact_role = 'MODEL' AND (
           artifact_record.metadata #>>
                '{logical_identity,candidate_selection_sha256}' !~ '^[0-9a-f]{64}$'
           OR artifact_record.metadata #>>
                '{logical_identity,candidate_selection_projection_sha256}'
                !~ '^[0-9a-f]{64}$'
           OR artifact_record.metadata #>>
                '{logical_identity,global_evidence_projection_sha256}'
                !~ '^[0-9a-f]{64}$'
           OR artifact_record.metadata #>>
                '{logical_identity,finalist_model_binding_sha256}' !~ '^[0-9a-f]{64}$'
           OR artifact_record.metadata #>>
                '{logical_identity,model_package_projection_sha256}' !~ '^[0-9a-f]{64}$'
           OR NOT systematic_fx.jsonb_has_exact_keys(
                artifact_record.metadata #> '{logical_identity,model_package_projection}',
                ARRAY[
                    'candidate_key', 'final_fit_model_count', 'finalist_model_binding_sha256',
                    'fit_keys', 'inner_model_count', 'model_sha256_by_fit_key',
                    'record_count', 'schema', 'wrapper_count',
                    'wrapper_sha256_by_fit_key'
                ]
              )
           OR artifact_record.metadata #>>
                '{logical_identity,model_package_projection,schema}' IS DISTINCT FROM
                'systematic_fx.bar_state_model_package_projection.v1'
           OR artifact_record.metadata #>>
                '{logical_identity,model_package_projection,candidate_key}'
                IS DISTINCT FROM candidate_key
           OR artifact_record.metadata #>>
                '{logical_identity,model_package_projection,inner_model_count}'
                IS DISTINCT FROM '3'
           OR artifact_record.metadata #>>
                '{logical_identity,model_package_projection,record_count}'
                IS DISTINCT FROM artifact_record.metadata #>> '{record_count}'
           OR artifact_record.metadata #>>
                '{logical_identity,model_package_projection,wrapper_count}'
                IS DISTINCT FROM artifact_record.metadata #>> '{record_count}'
           OR artifact_record.metadata #>>
                '{logical_identity,model_package_projection,finalist_model_binding_sha256}'
                IS DISTINCT FROM artifact_record.metadata #>>
                    '{logical_identity,finalist_model_binding_sha256}'
           OR systematic_fx.canonical_jsonb_sha256(
                artifact_record.metadata #> '{logical_identity,model_package_projection}'
              ) IS DISTINCT FROM artifact_record.metadata #>>
                '{logical_identity,model_package_projection_sha256}'
           OR jsonb_typeof(artifact_record.metadata #>
                '{logical_identity,model_package_projection,fit_keys}')
                IS DISTINCT FROM 'array'
           OR jsonb_typeof(artifact_record.metadata #>
                '{logical_identity,model_package_projection,model_sha256_by_fit_key}')
                IS DISTINCT FROM 'object'
           OR jsonb_typeof(artifact_record.metadata #>
                '{logical_identity,model_package_projection,wrapper_sha256_by_fit_key}')
                IS DISTINCT FROM 'object'
           OR (
                SELECT count(*) FROM jsonb_each_text(
                    artifact_record.metadata #>
                        '{logical_identity,model_package_projection,model_sha256_by_fit_key}'
                ) AS item(fit_key, sha256)
                WHERE item.sha256 !~ '^[0-9a-f]{64}$'
              ) <> 0
           OR NOT (
                artifact_record.metadata #>
                    '{logical_identity,model_package_projection,model_sha256_by_fit_key}'
                ?& ARRAY[
                    'discovery_inner_1', 'discovery_inner_2', 'discovery_inner_3'
                ]
              )
           OR NOT (
                artifact_record.metadata #>
                    '{logical_identity,model_package_projection,wrapper_sha256_by_fit_key}'
                ?& ARRAY[
                    'discovery_inner_1', 'discovery_inner_2', 'discovery_inner_3'
                ]
              )
           OR (
                SELECT count(*) FROM jsonb_each_text(
                    artifact_record.metadata #>
                        '{logical_identity,model_package_projection,wrapper_sha256_by_fit_key}'
                ) AS item(fit_key, sha256)
                WHERE item.sha256 !~ '^[0-9a-f]{64}$'
              ) <> 0
           OR NOT (
               (
                   artifact_record.metadata #> '{logical_identity,finalist_model_binding}'
                       = 'null'::jsonb
                   AND artifact_record.metadata #>>
                        '{logical_identity,finalist_model_binding_sha256}' =
                       '74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b'
                   AND artifact_record.metadata #>>
                        '{logical_identity,model_package_projection,final_fit_model_count}' = '0'
                   AND artifact_record.metadata #>
                        '{logical_identity,model_package_projection,fit_keys}' =
                        '["discovery_inner_1", "discovery_inner_2", "discovery_inner_3"]'::jsonb
               ) OR (
                   systematic_fx.jsonb_has_exact_keys(
                       artifact_record.metadata #>
                            '{logical_identity,finalist_model_binding}',
                       ARRAY[
                           'candidate_key', 'feature_set_id', 'model_sha256',
                           'timeframe_seconds'
                       ]
                   )
                   AND artifact_record.metadata #>>
                        '{logical_identity,finalist_model_binding,candidate_key}' = candidate_key
                   AND artifact_record.metadata #>>
                        '{logical_identity,finalist_model_binding,model_sha256}'
                       ~ '^[0-9a-f]{64}$'
                   AND artifact_record.metadata #>>
                        '{logical_identity,finalist_model_binding,timeframe_seconds}' =
                       substring(candidate_key FROM 8 FOR 4)::integer::text
                   AND lower(artifact_record.metadata #>>
                        '{logical_identity,finalist_model_binding,feature_set_id}') =
                       split_part(split_part(candidate_key, '_fs', 2), '_cm', 1)
                   AND systematic_fx.canonical_jsonb_sha256(
                       artifact_record.metadata #>
                            '{logical_identity,finalist_model_binding}'
                   ) = artifact_record.metadata #>>
                        '{logical_identity,finalist_model_binding_sha256}'
                   AND artifact_record.metadata #>>
                        '{logical_identity,model_package_projection,final_fit_model_count}' = '1'
                   AND artifact_record.metadata #>
                        '{logical_identity,model_package_projection,fit_keys}' =
                        '["discovery_inner_1", "discovery_inner_2", "discovery_inner_3", "discovery_final_fit"]'::jsonb
                   AND artifact_record.metadata #>>
                        '{logical_identity,model_package_projection,model_sha256_by_fit_key,discovery_final_fit}' =
                       artifact_record.metadata #>>
                        '{logical_identity,finalist_model_binding,model_sha256}'
                   AND artifact_record.metadata #>
                        '{logical_identity,model_package_projection,wrapper_sha256_by_fit_key}'
                       ? 'discovery_final_fit'
               )
           )
           OR jsonb_array_length(artifact_record.metadata #>
                '{logical_identity,model_package_projection,fit_keys}')
                IS DISTINCT FROM (artifact_record.metadata #>> '{record_count}')::integer
           OR (
                SELECT count(*) FROM jsonb_object_keys(
                    artifact_record.metadata #>
                        '{logical_identity,model_package_projection,model_sha256_by_fit_key}'
                )
              ) IS DISTINCT FROM (artifact_record.metadata #>> '{record_count}')::bigint
           OR (
                SELECT count(*) FROM jsonb_object_keys(
                    artifact_record.metadata #>
                        '{logical_identity,model_package_projection,wrapper_sha256_by_fit_key}'
                )
              ) IS DISTINCT FROM (artifact_record.metadata #>> '{record_count}')::bigint
       ) THEN
        RAISE EXCEPTION 'MODEL bar-state artifact semantic identity drifted';
    END IF;
    IF NEW.artifact_role = 'OOS_TRADE' AND (
           artifact_record.metadata #>> '{logical_identity,row_count}' !~ '^[0-9]+$'
           OR artifact_record.metadata #>> '{logical_identity,row_count}'
                IS DISTINCT FROM artifact_record.metadata #>> '{record_count}'
       ) THEN
        RAISE EXCEPTION 'OOS trade artifact row-count identity drifted';
    END IF;
    IF NEW.artifact_role = 'TERMINAL_RESULT' AND (
           artifact_record.metadata #>>
                '{logical_identity,compact_summary_sha256}' !~ '^[0-9a-f]{64}$'
           OR artifact_record.metadata #>>
                '{logical_identity,candidate_evidence_slice_sha256}' !~ '^[0-9a-f]{64}$'
           OR artifact_record.metadata #>>
                '{logical_identity,candidate_selection_sha256}' !~ '^[0-9a-f]{64}$'
           OR artifact_record.metadata #>>
                '{logical_identity,candidate_selection_projection_sha256}'
                !~ '^[0-9a-f]{64}$'
           OR artifact_record.metadata #>>
                '{logical_identity,finalist_model_binding_sha256}' !~ '^[0-9a-f]{64}$'
           OR artifact_record.metadata #>>
                '{logical_identity,global_evidence_projection_sha256}'
                !~ '^[0-9a-f]{64}$'
           OR artifact_record.metadata #>>
                '{logical_identity,model_package_projection_sha256}' !~ '^[0-9a-f]{64}$'
           OR (
               artifact_record.metadata #>> '{logical_identity,decision_label}'
                   IS DISTINCT FROM 'DISCOVERY_FINALIST'
               OR artifact_record.metadata #>> '{logical_identity,trial_status}'
                   IS DISTINCT FROM 'SUCCEEDED'
           ) AND (
               artifact_record.metadata #>> '{logical_identity,decision_label}'
                   IS DISTINCT FROM 'DISCOVERY_REJECT'
               OR artifact_record.metadata #>> '{logical_identity,trial_status}'
                   IS DISTINCT FROM 'REJECTED'
           )
           OR (
               artifact_record.metadata #>> '{logical_identity,trial_status}' = 'REJECTED'
               AND (
                   artifact_record.metadata #> '{logical_identity,finalist_model_binding}'
                       IS DISTINCT FROM 'null'::jsonb
                   OR artifact_record.metadata #>>
                        '{logical_identity,finalist_model_binding_sha256}'
                       IS DISTINCT FROM
                        '74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b'
               )
           )
           OR (
               artifact_record.metadata #>> '{logical_identity,trial_status}' = 'SUCCEEDED'
               AND (
                   NOT systematic_fx.jsonb_has_exact_keys(
                       artifact_record.metadata #>
                            '{logical_identity,finalist_model_binding}',
                       ARRAY[
                           'candidate_key', 'feature_set_id', 'model_sha256',
                           'timeframe_seconds'
                       ]
                   )
                   OR artifact_record.metadata #>>
                        '{logical_identity,finalist_model_binding,candidate_key}'
                       IS DISTINCT FROM candidate_key
                   OR artifact_record.metadata #>>
                        '{logical_identity,finalist_model_binding,model_sha256}'
                       !~ '^[0-9a-f]{64}$'
                   OR artifact_record.metadata #>>
                        '{logical_identity,finalist_model_binding,timeframe_seconds}'
                       IS DISTINCT FROM substring(candidate_key FROM 8 FOR 4)::integer::text
                   OR lower(artifact_record.metadata #>>
                        '{logical_identity,finalist_model_binding,feature_set_id}')
                       IS DISTINCT FROM split_part(split_part(candidate_key, '_fs', 2), '_cm', 1)
                   OR systematic_fx.canonical_jsonb_sha256(
                       artifact_record.metadata #>
                            '{logical_identity,finalist_model_binding}'
                   ) IS DISTINCT FROM artifact_record.metadata #>>
                        '{logical_identity,finalist_model_binding_sha256}'
               )
           )
       ) THEN
        RAISE EXCEPTION 'terminal bar-state artifact decision/status drifted';
    END IF;
    IF NEW.artifact_role = 'GLOBAL_RESULT' THEN
        IF artifact_record.metadata #>>
                '{logical_identity,global_evidence_projection_sha256}'
                !~ '^[0-9a-f]{64}$'
           OR jsonb_typeof(
               artifact_record.metadata #>
                    '{logical_identity,candidate_evidence_slice_sha256_by_key}'
           ) IS DISTINCT FROM 'object'
           OR (
               SELECT count(*)
               FROM jsonb_object_keys(
                   artifact_record.metadata #>
                        '{logical_identity,candidate_evidence_slice_sha256_by_key}'
               )
           ) <> 12
           OR jsonb_typeof(
               artifact_record.metadata #>
                    '{logical_identity,candidate_oos_trade_record_count_by_key}'
           ) IS DISTINCT FROM 'object'
           OR (
               SELECT count(*)
               FROM jsonb_object_keys(
                   artifact_record.metadata #>
                        '{logical_identity,candidate_oos_trade_record_count_by_key}'
               )
           ) <> 12
           OR jsonb_typeof(
               artifact_record.metadata #>
                    '{logical_identity,candidate_selection_sha256_by_key}'
           ) IS DISTINCT FROM 'object'
           OR (
               SELECT count(*)
               FROM jsonb_object_keys(
                   artifact_record.metadata #>
                        '{logical_identity,candidate_selection_sha256_by_key}'
               )
           ) <> 12
           OR jsonb_typeof(
               artifact_record.metadata #>
                    '{logical_identity,candidate_selection_projection_sha256_by_key}'
           ) IS DISTINCT FROM 'object'
           OR (
               SELECT count(*)
               FROM jsonb_object_keys(
                   artifact_record.metadata #>
                        '{logical_identity,candidate_selection_projection_sha256_by_key}'
               )
           ) <> 12
           OR jsonb_typeof(
               artifact_record.metadata #>
                    '{logical_identity,model_package_projection_sha256_by_key}'
           ) IS DISTINCT FROM 'object'
           OR (
               SELECT count(*)
               FROM jsonb_object_keys(
                   artifact_record.metadata #>
                        '{logical_identity,model_package_projection_sha256_by_key}'
               )
           ) <> 12
           OR jsonb_typeof(
               artifact_record.metadata #>
                    '{logical_identity,finalist_model_binding_by_key}'
           ) IS DISTINCT FROM 'object'
           OR (
               SELECT count(*)
               FROM jsonb_object_keys(
                   artifact_record.metadata #>
                        '{logical_identity,finalist_model_binding_by_key}'
               )
           ) <> 12
           OR jsonb_typeof(
               artifact_record.metadata #>
                    '{logical_identity,finalist_model_binding_sha256_by_key}'
           ) IS DISTINCT FROM 'object'
           OR (
               SELECT count(*)
               FROM jsonb_object_keys(
                   artifact_record.metadata #>
                        '{logical_identity,finalist_model_binding_sha256_by_key}'
               )
           ) <> 12
           OR EXISTS (
               SELECT 1
               FROM jsonb_each_text(
                   artifact_record.metadata #>
                        '{logical_identity,candidate_evidence_slice_sha256_by_key}'
               ) AS item(candidate, slice_sha256)
               WHERE item.candidate !~
                       '^bsv2_tf(0300|1800)_fs(morphology|state)_cm(005|010|015)$'
                  OR item.slice_sha256 !~ '^[0-9a-f]{64}$'
           )
           OR EXISTS (
               SELECT 1
               FROM jsonb_each(
                   artifact_record.metadata #>
                        '{logical_identity,candidate_oos_trade_record_count_by_key}'
               ) AS item(candidate, record_count)
               WHERE item.candidate !~
                       '^bsv2_tf(0300|1800)_fs(morphology|state)_cm(005|010|015)$'
                  OR jsonb_typeof(item.record_count) IS DISTINCT FROM 'number'
                  OR item.record_count::text !~ '^(0|[1-9][0-9]*)$'
           )
           OR EXISTS (
               SELECT 1
               FROM jsonb_each_text(
                   artifact_record.metadata #>
                        '{logical_identity,candidate_selection_sha256_by_key}'
               ) AS item(candidate, selection_sha256)
               WHERE item.candidate !~
                       '^bsv2_tf(0300|1800)_fs(morphology|state)_cm(005|010|015)$'
                  OR item.selection_sha256 !~ '^[0-9a-f]{64}$'
           )
           OR EXISTS (
               SELECT 1
               FROM jsonb_each_text(
                   artifact_record.metadata #>
                        '{logical_identity,candidate_selection_projection_sha256_by_key}'
               ) AS item(candidate, selection_sha256)
               WHERE item.candidate !~
                       '^bsv2_tf(0300|1800)_fs(morphology|state)_cm(005|010|015)$'
                  OR item.selection_sha256 !~ '^[0-9a-f]{64}$'
           )
           OR EXISTS (
               SELECT 1
               FROM jsonb_each_text(
                   artifact_record.metadata #>
                        '{logical_identity,model_package_projection_sha256_by_key}'
               ) AS item(candidate, package_sha256)
               WHERE item.candidate !~
                       '^bsv2_tf(0300|1800)_fs(morphology|state)_cm(005|010|015)$'
                  OR item.package_sha256 !~ '^[0-9a-f]{64}$'
           )
           OR EXISTS (
               SELECT 1
               FROM jsonb_each(
                   artifact_record.metadata #>
                        '{logical_identity,finalist_model_binding_by_key}'
               ) AS item(candidate, binding)
               WHERE item.candidate !~
                       '^bsv2_tf(0300|1800)_fs(morphology|state)_cm(005|010|015)$'
                  OR NOT (
                      (
                          item.binding = 'null'::jsonb
                          AND artifact_record.metadata #>
                                '{logical_identity,finalist_model_binding_sha256_by_key}'
                                ->> item.candidate =
                              '74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b'
                      ) OR (
                          systematic_fx.jsonb_has_exact_keys(
                              item.binding,
                              ARRAY[
                                  'candidate_key', 'feature_set_id', 'model_sha256',
                                  'timeframe_seconds'
                              ]
                          )
                          AND item.binding #>> '{candidate_key}' = item.candidate
                          AND item.binding #>> '{model_sha256}' ~ '^[0-9a-f]{64}$'
                          AND item.binding #>> '{timeframe_seconds}' =
                              substring(item.candidate FROM 8 FOR 4)::integer::text
                          AND lower(item.binding #>> '{feature_set_id}') =
                              split_part(split_part(item.candidate, '_fs', 2), '_cm', 1)
                          AND systematic_fx.canonical_jsonb_sha256(item.binding) =
                              artifact_record.metadata #>
                                '{logical_identity,finalist_model_binding_sha256_by_key}'
                                ->> item.candidate
                      )
                  )
           )
           OR EXISTS (
               SELECT 1
               FROM jsonb_each_text(
                   artifact_record.metadata #>
                        '{logical_identity,finalist_model_binding_sha256_by_key}'
               ) AS item(candidate, binding_sha256)
               WHERE item.candidate !~
                       '^bsv2_tf(0300|1800)_fs(morphology|state)_cm(005|010|015)$'
                  OR item.binding_sha256 !~ '^[0-9a-f]{64}$'
                  OR NOT (
                      artifact_record.metadata #>
                        '{logical_identity,finalist_model_binding_by_key}'
                        ? item.candidate
                  )
           )
           OR (
               SELECT count(*)
               FROM jsonb_each(
                   artifact_record.metadata #>
                        '{logical_identity,finalist_model_binding_by_key}'
               ) AS item(candidate, binding)
               WHERE item.binding <> 'null'::jsonb
        ) > 4 THEN
            RAISE EXCEPTION 'global bar-state artifact semantic hash catalog drifted';
        END IF;
        PERFORM campaign.campaign_id
        FROM systematic_fx.campaigns AS campaign
        WHERE campaign.campaign_id = NEW.campaign_id
          AND campaign.campaign_key = profile_campaign_key
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'global bar-state artifact requires its exact campaign';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM systematic_fx.bar_state_artifact_links AS prior
            WHERE prior.campaign_id = NEW.campaign_id
              AND prior.artifact_role = 'GLOBAL_RESULT'
              AND (
                  prior.artifact_identity_sha256 IS DISTINCT FROM
                      NEW.artifact_identity_sha256
                  OR prior.content_sha256 IS DISTINCT FROM NEW.content_sha256
              )
        ) THEN
            RAISE EXCEPTION 'bar-state candidates require one exact global result';
        END IF;
    END IF;
    IF NEW.artifact_role = 'TERMINAL_RESULT' AND NOT EXISTS (
        SELECT 1
        FROM systematic_fx.bar_state_artifact_links AS global_link
        JOIN systematic_fx.artifacts AS global_artifact
          ON global_artifact.artifact_id = global_link.artifact_id
        JOIN systematic_fx.bar_state_artifact_links AS model_link
          ON model_link.research_run_attempt_id =
             global_link.research_run_attempt_id
         AND model_link.artifact_role = 'MODEL'
         AND model_link.split_key = 'discovery'
         AND model_link.shard_ordinal = 0
        JOIN systematic_fx.artifacts AS model_artifact
          ON model_artifact.artifact_id = model_link.artifact_id
        JOIN systematic_fx.bar_state_artifact_links AS oos_link
          ON oos_link.research_run_attempt_id = global_link.research_run_attempt_id
         AND oos_link.artifact_role = 'OOS_TRADE'
         AND oos_link.split_key = 'discovery'
         AND oos_link.shard_ordinal = 0
        JOIN systematic_fx.artifacts AS oos_artifact
          ON oos_artifact.artifact_id = oos_link.artifact_id
        WHERE global_link.research_run_attempt_id = NEW.research_run_attempt_id
          AND global_link.research_run_spec_id = NEW.research_run_spec_id
          AND global_link.experiment_trial_id = NEW.experiment_trial_id
          AND global_link.artifact_role = 'GLOBAL_RESULT'
          AND global_link.split_key = 'discovery'
          AND global_link.shard_ordinal = 0
          AND global_artifact.metadata #>
                '{logical_identity,candidate_evidence_slice_sha256_by_key}'
                ->> candidate_key IS NOT DISTINCT FROM
              artifact_record.metadata #>>
                '{logical_identity,candidate_evidence_slice_sha256}'
          AND global_artifact.metadata #>
                '{logical_identity,candidate_selection_sha256_by_key}'
                ->> candidate_key IS NOT DISTINCT FROM
              artifact_record.metadata #>>
                '{logical_identity,candidate_selection_sha256}'
          AND global_artifact.metadata #>
                '{logical_identity,candidate_selection_projection_sha256_by_key}'
                ->> candidate_key IS NOT DISTINCT FROM
              artifact_record.metadata #>>
                '{logical_identity,candidate_selection_projection_sha256}'
          AND global_artifact.metadata #>>
                '{logical_identity,global_evidence_projection_sha256}'
              IS NOT DISTINCT FROM artifact_record.metadata #>>
                '{logical_identity,global_evidence_projection_sha256}'
          AND global_artifact.metadata #>
                '{logical_identity,model_package_projection_sha256_by_key}'
                ->> candidate_key IS NOT DISTINCT FROM
              artifact_record.metadata #>>
                '{logical_identity,model_package_projection_sha256}'
          AND global_artifact.metadata #>
                '{logical_identity,finalist_model_binding_by_key}'
                -> candidate_key IS NOT DISTINCT FROM
              artifact_record.metadata #>
                '{logical_identity,finalist_model_binding}'
          AND global_artifact.metadata #>
                '{logical_identity,finalist_model_binding_sha256_by_key}'
                ->> candidate_key IS NOT DISTINCT FROM
              artifact_record.metadata #>>
                '{logical_identity,finalist_model_binding_sha256}'
          AND global_artifact.metadata #>
                '{logical_identity,candidate_oos_trade_record_count_by_key}'
                ->> candidate_key IS NOT DISTINCT FROM
              oos_artifact.metadata #>> '{record_count}'
          AND oos_artifact.metadata #>> '{logical_identity,row_count}'
                IS NOT DISTINCT FROM oos_artifact.metadata #>> '{record_count}'
          AND model_artifact.metadata #>>
                '{logical_identity,candidate_selection_sha256}'
                IS NOT DISTINCT FROM artifact_record.metadata #>>
                    '{logical_identity,candidate_selection_sha256}'
          AND model_artifact.metadata #>>
                '{logical_identity,candidate_selection_projection_sha256}'
                IS NOT DISTINCT FROM artifact_record.metadata #>>
                    '{logical_identity,candidate_selection_projection_sha256}'
          AND model_artifact.metadata #>>
                '{logical_identity,global_evidence_projection_sha256}'
                IS NOT DISTINCT FROM artifact_record.metadata #>>
                    '{logical_identity,global_evidence_projection_sha256}'
          AND model_artifact.metadata #>>
                '{logical_identity,model_package_projection_sha256}'
                IS NOT DISTINCT FROM artifact_record.metadata #>>
                    '{logical_identity,model_package_projection_sha256}'
          AND model_artifact.metadata #>
                '{logical_identity,finalist_model_binding}'
                IS NOT DISTINCT FROM artifact_record.metadata #>
                    '{logical_identity,finalist_model_binding}'
          AND model_artifact.metadata #>>
                '{logical_identity,finalist_model_binding_sha256}'
                IS NOT DISTINCT FROM artifact_record.metadata #>>
                    '{logical_identity,finalist_model_binding_sha256}'
    ) THEN
        RAISE EXCEPTION 'terminal bar-state artifact differs from GLOBAL semantic binding';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION systematic_fx.require_bar_state_terminal_pair()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    target_spec_id bigint;
    candidate_key text;
    terminal_attempts integer;
    terminal_trials integer;
BEGIN
    IF TG_TABLE_NAME = 'research_run_attempts' THEN
        IF NEW.status IS DISTINCT FROM 'SUCCEEDED' THEN RETURN NULL; END IF;
        target_spec_id := NEW.research_run_spec_id;
    ELSE
        IF NEW.status NOT IN ('SUCCEEDED', 'REJECTED') THEN RETURN NULL; END IF;
        target_spec_id := NEW.research_run_spec_id;
    END IF;
    IF target_spec_id IS NULL THEN
        IF TG_TABLE_NAME = 'experiment_trials'
           AND systematic_fx.bar_state_experiment_is_governed(NEW.experiment_id) THEN
            RAISE EXCEPTION 'terminal bar-state trial requires its exact RunSpec';
        END IF;
        RETURN NULL;
    END IF;

    SELECT run_spec.canonical_spec #>> '{parameters,bar_state_candidate_key}'
    INTO candidate_key
    FROM systematic_fx.research_run_specs AS run_spec
    JOIN systematic_fx.campaigns AS campaign
      ON campaign.campaign_id = run_spec.campaign_id
    WHERE run_spec.research_run_spec_id = target_spec_id
      AND EXISTS (
          SELECT 1
          FROM systematic_fx.bar_state_governance_profile(campaign.campaign_key)
      );
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    SELECT count(*)::integer INTO terminal_attempts
    FROM systematic_fx.research_run_attempts AS attempt
    JOIN systematic_fx.experiment_trials AS trial
      ON trial.research_run_spec_id = attempt.research_run_spec_id
     AND trial.trial_key = candidate_key
    JOIN systematic_fx.bar_state_artifact_links AS terminal_link
      ON terminal_link.research_run_attempt_id = attempt.research_run_attempt_id
     AND terminal_link.research_run_spec_id = attempt.research_run_spec_id
     AND terminal_link.experiment_trial_id = trial.experiment_trial_id
     AND terminal_link.artifact_role = 'TERMINAL_RESULT'
     AND terminal_link.split_key = 'discovery'
     AND terminal_link.shard_ordinal = 0
    JOIN systematic_fx.artifacts AS terminal_artifact
      ON terminal_artifact.artifact_id = terminal_link.artifact_id
    JOIN systematic_fx.bar_state_artifact_links AS global_link
      ON global_link.research_run_attempt_id = attempt.research_run_attempt_id
     AND global_link.artifact_role = 'GLOBAL_RESULT'
     AND global_link.split_key = 'discovery'
     AND global_link.shard_ordinal = 0
    JOIN systematic_fx.artifacts AS global_artifact
      ON global_artifact.artifact_id = global_link.artifact_id
    JOIN systematic_fx.bar_state_artifact_links AS model_link
      ON model_link.research_run_attempt_id = attempt.research_run_attempt_id
     AND model_link.artifact_role = 'MODEL'
     AND model_link.split_key = 'discovery'
     AND model_link.shard_ordinal = 0
    JOIN systematic_fx.artifacts AS model_artifact
      ON model_artifact.artifact_id = model_link.artifact_id
    JOIN systematic_fx.bar_state_artifact_links AS oos_link
      ON oos_link.research_run_attempt_id = attempt.research_run_attempt_id
     AND oos_link.artifact_role = 'OOS_TRADE'
     AND oos_link.split_key = 'discovery'
     AND oos_link.shard_ordinal = 0
    JOIN systematic_fx.artifacts AS oos_artifact
      ON oos_artifact.artifact_id = oos_link.artifact_id
    WHERE attempt.research_run_spec_id = target_spec_id
      AND attempt.status = 'SUCCEEDED'
      AND trial.status IN ('SUCCEEDED', 'REJECTED')
      AND attempt.result_summary = trial.result_summary
      AND attempt.result_artifact_id = terminal_link.artifact_id
      AND attempt.result_summary #>> '{result_artifact_id}' =
          terminal_link.artifact_id::text
      AND attempt.result_summary #>> '{candidate_key}' = candidate_key
      AND attempt.result_summary #>> '{trial_status}' = trial.status
      AND (
          (
              trial.status = 'SUCCEEDED'
              AND attempt.result_summary #>> '{decision_label}' =
                  'DISCOVERY_FINALIST'
          ) OR (
              trial.status = 'REJECTED'
              AND attempt.result_summary #>> '{decision_label}' =
                  'DISCOVERY_REJECT'
          )
      )
      AND terminal_artifact.metadata #>> '{logical_identity,candidate_key}' =
          candidate_key
      AND terminal_artifact.metadata #>> '{logical_identity,decision_label}' =
          attempt.result_summary #>> '{decision_label}'
      AND terminal_artifact.metadata #>> '{logical_identity,trial_status}' =
          trial.status
      AND terminal_artifact.metadata #>>
            '{logical_identity,compact_summary_sha256}' =
          systematic_fx.canonical_jsonb_sha256(
              attempt.result_summary -> 'compact_summary'
          )
      AND terminal_artifact.metadata #>>
            '{logical_identity,candidate_evidence_slice_sha256}' =
          attempt.result_summary #>> '{candidate_evidence_slice_sha256}'
      AND terminal_artifact.metadata #>>
            '{logical_identity,candidate_selection_sha256}' =
          attempt.result_summary #>> '{candidate_selection_sha256}'
      AND terminal_artifact.metadata #>>
            '{logical_identity,candidate_selection_projection_sha256}' =
          attempt.result_summary #>> '{candidate_selection_projection_sha256}'
      AND terminal_artifact.metadata #>>
            '{logical_identity,global_evidence_projection_sha256}' =
          attempt.result_summary #>> '{global_evidence_projection_sha256}'
      AND terminal_artifact.metadata #>>
            '{logical_identity,model_package_projection_sha256}' =
          attempt.result_summary #>> '{model_package_projection_sha256}'
      AND terminal_artifact.metadata #>>
            '{logical_identity,finalist_model_binding_sha256}' =
          attempt.result_summary #>> '{finalist_model_binding_sha256}'
      AND model_artifact.metadata #>>
            '{logical_identity,candidate_selection_sha256}' =
          attempt.result_summary #>> '{candidate_selection_sha256}'
      AND model_artifact.metadata #>>
            '{logical_identity,candidate_selection_projection_sha256}' =
          attempt.result_summary #>> '{candidate_selection_projection_sha256}'
      AND model_artifact.metadata #>>
            '{logical_identity,global_evidence_projection_sha256}' =
          attempt.result_summary #>> '{global_evidence_projection_sha256}'
      AND model_artifact.metadata #>>
            '{logical_identity,model_package_projection_sha256}' =
          attempt.result_summary #>> '{model_package_projection_sha256}'
      AND model_artifact.metadata #> '{logical_identity,finalist_model_binding}' =
          terminal_artifact.metadata #> '{logical_identity,finalist_model_binding}'
      AND model_artifact.metadata #>>
            '{logical_identity,finalist_model_binding_sha256}' =
          attempt.result_summary #>> '{finalist_model_binding_sha256}'
      AND global_artifact.metadata #>
            '{logical_identity,candidate_evidence_slice_sha256_by_key}'
            ->> candidate_key =
          attempt.result_summary #>> '{candidate_evidence_slice_sha256}'
      AND global_artifact.metadata #>
            '{logical_identity,candidate_selection_sha256_by_key}'
            ->> candidate_key =
          attempt.result_summary #>> '{candidate_selection_sha256}'
      AND global_artifact.metadata #>
            '{logical_identity,candidate_selection_projection_sha256_by_key}'
            ->> candidate_key =
          attempt.result_summary #>> '{candidate_selection_projection_sha256}'
      AND global_artifact.metadata #>>
            '{logical_identity,global_evidence_projection_sha256}' =
          attempt.result_summary #>> '{global_evidence_projection_sha256}'
      AND global_artifact.metadata #>
            '{logical_identity,model_package_projection_sha256_by_key}'
            ->> candidate_key =
          attempt.result_summary #>> '{model_package_projection_sha256}'
      AND global_artifact.metadata #>
            '{logical_identity,finalist_model_binding_by_key}'
            -> candidate_key =
          terminal_artifact.metadata #> '{logical_identity,finalist_model_binding}'
      AND global_artifact.metadata #>
            '{logical_identity,finalist_model_binding_sha256_by_key}'
            ->> candidate_key =
          attempt.result_summary #>> '{finalist_model_binding_sha256}'
      AND global_artifact.metadata #>
            '{logical_identity,candidate_oos_trade_record_count_by_key}'
            ->> candidate_key = oos_artifact.metadata #>> '{record_count}'
      AND oos_artifact.metadata #>> '{logical_identity,row_count}' =
          oos_artifact.metadata #>> '{record_count}';
    SELECT count(*) INTO terminal_trials
    FROM systematic_fx.experiment_trials AS trial
    WHERE trial.research_run_spec_id = target_spec_id
      AND trial.trial_key = candidate_key
      AND trial.status IN ('SUCCEEDED', 'REJECTED');
    IF terminal_attempts <> 1 OR terminal_trials <> 1 THEN
        RAISE EXCEPTION 'bar-state terminal RunSpec requires one exact attempt/trial pair';
    END IF;
    RETURN NULL;
END;
$$;

COMMENT ON FUNCTION systematic_fx.bar_state_governance_profile(text) IS
    'Immutable dual-profile identity lookup for governed State V2 and V2A campaigns.';
COMMENT ON FUNCTION systematic_fx.bar_state_v2a_predecessor_is_clean() IS
    'Fails closed unless exact State V2 ended with twelve clean FAILED attempts and no research evidence.';

INSERT INTO systematic_fx.publication_outbox (
    scope_key, request_version, delivered_version, requested_at
)
VALUES ('public-research', 1, 0, statement_timestamp())
ON CONFLICT (scope_key) DO UPDATE
SET request_version = systematic_fx.publication_outbox.request_version + 1,
    requested_at = statement_timestamp(),
    last_error = NULL;

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (26, 'bar_state_v2a_optimizer_cap_amendment', :'migration_checksum');

COMMIT;
