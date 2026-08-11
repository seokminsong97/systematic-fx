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
        ),
        (
            'V2B'::text,
            'bar_state_conditional_v2b'::text,
            'Frozen conditional candle-state Discovery v2b'::text,
            'bar_state_conditional_v2b:experiment:frozen_candidate_catalog:v1'::text,
            'bar_state_conditional_v2b'::text,
            'bar_state_conditional_discovery_v2b'::text,
            '87127f274ef4cc500deede2d8031919711c042530711051ba7ec598cda4e021e'::text,
            '547f30350eb829d5cf82bef6c62e7720ac9a81511759e3a791cdeba24245ad09'::text,
            '97bbdacd0d655a1ca4e81085f3f25fb32da0bf31329bbd670ba89778611084d6'::text,
            'cee6838d9c85498818140bd02ae92483fe17c080d4909190eb0b83f790e5bb60'::text,
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
            'bar_state_conditional_v2a'::text,
            '8a332ad6998bb8bf48c3de94bc0ca660905a08acb848580ee5e31d9c42f8033c'::text,
            '8688c7efb298f9644ee3821ce575349c446c6998'::text,
            'REQUIRE_EXACT_FAILED_PREDECESSOR_ATTEMPTS_1_AND_2_WITH_NO_GOVERNED_EVIDENCE'::text
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

CREATE FUNCTION systematic_fx.bar_state_preregistration_is_exact(
    target_campaign_key text,
    target_experiment_id bigint
)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
    WITH context AS (
        SELECT profile.*, campaign.code_commit,
               experiment.registration_artifact_id
        FROM systematic_fx.campaigns AS campaign
        JOIN systematic_fx.experiments AS experiment
          ON experiment.campaign_id = campaign.campaign_id
        JOIN LATERAL systematic_fx.bar_state_governance_profile(
            campaign.campaign_key
        ) AS profile ON profile.experiment_key = experiment.experiment_key
        WHERE campaign.campaign_key = target_campaign_key
          AND experiment.experiment_id = target_experiment_id
    ),
    exact_code AS (
        SELECT artifact.artifact_id, artifact.artifact_key,
               artifact.sha256, artifact.byte_size,
               artifact.metadata #>> '{artifact_identity_sha256}'
                   AS artifact_identity_sha256,
               artifact.metadata #> '{logical_identity,lineage}' AS lineage
        FROM systematic_fx.artifacts AS artifact
        JOIN context ON context.artifact_type = artifact.artifact_type
        WHERE artifact.artifact_key =
                  context.artifact_type || ':code_snapshot:' || artifact.sha256
          AND artifact.media_type = 'application/json'
          AND artifact.sha256 = artifact.metadata #>> '{content_sha256}'
          AND artifact.artifact_key = artifact.metadata #>> '{artifact_key}'
          AND artifact.artifact_type = artifact.metadata #>> '{artifact_type}'
          AND artifact.metadata #>> '{artifact_schema}' =
              'systematic_fx.code_snapshot.v2'
          AND artifact.metadata #> '{artifact_version}' = '1'::jsonb
          AND artifact.metadata #>> '{record_count}' ~ '^(0|[1-9][0-9]*)$'
          AND artifact.metadata #>> '{schema_sha256}' ~ '^[0-9a-f]{64}$'
          AND artifact.metadata #>> '{file_suffix}' = '.json'
          AND artifact.metadata #>> '{media_type}' = 'application/json'
          AND artifact.metadata #>> '{identity_schema}' =
              'systematic_fx.bar_artifact_identity.v1'
          AND artifact.metadata #>> '{root_kind}' = 'bar_patterns'
          AND artifact.metadata #>> '{source_manifest_sha256}' =
              'e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc'
          AND artifact.metadata #>> '{logical_identity,artifact_kind}' =
              'CODE_SNAPSHOT'
          AND artifact.metadata #>> '{logical_identity,campaign_key}' =
              context.campaign_key
          AND artifact.metadata #>> '{logical_identity,code_commit}' =
              context.code_commit
          AND artifact.metadata #>>
                  '{logical_identity,code_snapshot_sha256}' = artifact.sha256
          AND systematic_fx.jsonb_has_exact_keys(
              artifact.metadata,
              ARRAY[
                  'artifact_identity_sha256', 'artifact_key',
                  'artifact_schema', 'artifact_type', 'artifact_version',
                  'content_sha256', 'file_suffix', 'identity_schema',
                  'logical_identity', 'media_type', 'record_count',
                  'root_kind', 'schema_sha256', 'source_manifest_sha256'
              ]
          )
          AND systematic_fx.jsonb_has_exact_keys(
              artifact.metadata #> '{logical_identity}',
              ARRAY[
                  'artifact_kind', 'campaign_key', 'code_commit',
                  'code_snapshot_sha256', 'lineage', 'lineage_sha256'
              ]
          )
          AND systematic_fx.jsonb_has_exact_keys(
              artifact.metadata #> '{logical_identity,lineage}',
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
          AND artifact.metadata #>> '{logical_identity,lineage,schema}' =
              'systematic_fx.bar_state_artifact_lineage.v1'
          AND artifact.metadata #>>
                  '{logical_identity,lineage,bar_dataset_manifest_sha256}' =
              'e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc'
          AND artifact.metadata #>>
                  '{logical_identity,lineage,raw_source_manifest_sha256}' =
              '14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de'
          AND artifact.metadata #>>
                  '{logical_identity,lineage,config_file_sha256}' =
              context.config_file_sha256
          AND artifact.metadata #>>
                  '{logical_identity,lineage,config_semantic_sha256}' =
              context.config_semantic_sha256
          AND artifact.metadata #>>
                  '{logical_identity,lineage,candidate_catalog_sha256}' =
              context.candidate_catalog_sha256
          AND artifact.metadata #>>
                  '{logical_identity,lineage,training_plan_sha256}' =
              '9a4833aa53fe03788ddf224efcd24abbcc492498b915f174b6473f9c37f3fc8a'
          AND artifact.metadata #>>
                  '{logical_identity,lineage,code_snapshot_sha256}' = artifact.sha256
          AND artifact.metadata #>>
                  '{logical_identity,lineage,dependency_lock_sha256}' ~
              '^[0-9a-f]{64}$'
          AND artifact.metadata #>>
                  '{logical_identity,lineage,runtime_environment_sha256}' ~
              '^[0-9a-f]{64}$'
          AND artifact.metadata #>>
                  '{logical_identity,lineage,ordered_run_set_sha256}' ~
              '^[0-9a-f]{64}$'
          AND artifact.metadata #>>
                  '{logical_identity,lineage,discovery_scope_sha256}' =
              '35e59a3475d9e79e17e1b132b6a2044458f46069a715ceb7c458ad298cab3ec0'
          AND systematic_fx.canonical_jsonb_sha256(
                  artifact.metadata #> '{logical_identity,lineage,discovery_scope}'
              ) =
              '35e59a3475d9e79e17e1b132b6a2044458f46069a715ceb7c458ad298cab3ec0'
          AND artifact.metadata #>
                  '{logical_identity,lineage,candidate_definition_sha256}' =
              'null'::jsonb
          AND artifact.metadata #> '{logical_identity,lineage,candidate_key}' =
              'null'::jsonb
          AND artifact.metadata #>
                  '{logical_identity,lineage,run_fingerprint}' = 'null'::jsonb
          AND artifact.metadata #>
                  '{logical_identity,lineage,parent_artifacts}' = '[]'::jsonb
          AND artifact.metadata #>> '{logical_identity,lineage_sha256}' =
              systematic_fx.canonical_jsonb_sha256(
                  artifact.metadata #> '{logical_identity,lineage}'
              )
          AND artifact.metadata #>> '{artifact_identity_sha256}' =
              systematic_fx.canonical_jsonb_sha256(
                  artifact.metadata - 'artifact_identity_sha256' - 'content_sha256'
              )
    ),
    exact_registration AS (
        SELECT registration.artifact_id
        FROM systematic_fx.artifacts AS registration
        JOIN context ON context.registration_artifact_id = registration.artifact_id
        JOIN exact_code AS code ON true
        WHERE registration.artifact_type = context.artifact_type
          AND registration.artifact_key =
              context.artifact_type || ':registration:' ||
                  context.campaign_definition_sha256
          AND registration.media_type = 'application/json'
          AND registration.sha256 = registration.metadata #>> '{content_sha256}'
          AND registration.artifact_key = registration.metadata #>> '{artifact_key}'
          AND registration.artifact_type = registration.metadata #>> '{artifact_type}'
          AND registration.metadata #>> '{artifact_schema}' =
              'systematic_fx.bar_state_registration_artifact.v1'
          AND registration.metadata #> '{artifact_version}' = '1'::jsonb
          AND registration.metadata #> '{record_count}' = '12'::jsonb
          AND registration.metadata #>> '{schema_sha256}' ~ '^[0-9a-f]{64}$'
          AND registration.metadata #>> '{file_suffix}' = '.json'
          AND registration.metadata #>> '{media_type}' = 'application/json'
          AND registration.metadata #>> '{identity_schema}' =
              'systematic_fx.bar_artifact_identity.v1'
          AND registration.metadata #>> '{root_kind}' = 'bar_patterns'
          AND registration.metadata #>> '{source_manifest_sha256}' =
              'e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc'
          AND registration.metadata #>> '{logical_identity,artifact_kind}' =
              'REGISTRATION'
          AND registration.metadata #>> '{logical_identity,campaign_key}' =
              context.campaign_key
          AND registration.metadata #>>
                  '{logical_identity,campaign_definition_sha256}' =
              context.campaign_definition_sha256
          AND registration.metadata #>>
                  '{logical_identity,candidate_catalog_sha256}' =
              context.candidate_catalog_sha256
          AND systematic_fx.jsonb_has_exact_keys(
              registration.metadata,
              ARRAY[
                  'artifact_identity_sha256', 'artifact_key',
                  'artifact_schema', 'artifact_type', 'artifact_version',
                  'content_sha256', 'file_suffix', 'identity_schema',
                  'logical_identity', 'media_type', 'record_count',
                  'root_kind', 'schema_sha256', 'source_manifest_sha256'
              ]
          )
          AND systematic_fx.jsonb_has_exact_keys(
              registration.metadata #> '{logical_identity}',
              ARRAY[
                  'artifact_kind', 'campaign_definition_sha256',
                  'campaign_key', 'candidate_catalog_sha256', 'lineage',
                  'lineage_sha256'
              ]
          )
          AND systematic_fx.jsonb_has_exact_keys(
              registration.metadata #> '{logical_identity,lineage}',
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
          AND (
              (registration.metadata #> '{logical_identity,lineage}') -
                  'parent_artifacts'
          ) = (code.lineage - 'parent_artifacts')
          AND registration.metadata #>
                  '{logical_identity,lineage,parent_artifacts}' =
              jsonb_build_array(
                  jsonb_build_object(
                      'artifact_identity_sha256', code.artifact_identity_sha256,
                      'artifact_key', code.artifact_key,
                      'byte_size', code.byte_size,
                      'content_sha256', code.sha256,
                      'schema', 'systematic_fx.bar_state_parent_artifact.v1'
                  )
              )
          AND registration.metadata #>> '{logical_identity,lineage_sha256}' =
              systematic_fx.canonical_jsonb_sha256(
                  registration.metadata #> '{logical_identity,lineage}'
              )
          AND registration.metadata #>> '{artifact_identity_sha256}' =
              systematic_fx.canonical_jsonb_sha256(
                  registration.metadata - 'artifact_identity_sha256' -
                      'content_sha256'
              )
    )
    SELECT EXISTS (SELECT 1 FROM context)
       AND (SELECT count(*) FROM exact_code) = 1
       AND (SELECT count(*) FROM exact_registration) = 1
       AND (
           SELECT count(*)
           FROM systematic_fx.artifacts AS artifact
           JOIN context ON context.artifact_type = artifact.artifact_type
       ) = 2;
$$;

ALTER FUNCTION systematic_fx.bar_state_v2a_predecessor_is_clean()
    RENAME TO bar_state_v2a_predecessor_is_clean_v26;

CREATE FUNCTION systematic_fx.bar_state_v2a_predecessor_is_clean()
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
    SELECT systematic_fx.bar_state_v2a_predecessor_is_clean_v26()
       AND EXISTS (
           SELECT 1
           FROM systematic_fx.campaigns AS campaign
           JOIN systematic_fx.experiments AS experiment
             ON experiment.campaign_id = campaign.campaign_id
           WHERE campaign.campaign_key = 'bar_state_conditional_v2'
             AND experiment.experiment_key =
                 'bar_state_conditional_v2:experiment:frozen_candidate_catalog:v1'
             AND campaign.selected_start_date = DATE '2022-01-03'
             AND campaign.selected_end_date = DATE '2026-07-31'
             AND campaign.roll_cutoff_date IS NULL
             AND systematic_fx.canonical_jsonb_sha256(campaign.split_policy) =
                 '5da1027fb2003c521b4be2eee0d2bf1238e4784467f43f7d9b9ac978223f5552'
             AND experiment.pattern_id IS NULL
             AND experiment.parent_experiment_id IS NULL
             AND experiment.hypothesis =
                 'Completed candle state predicts next-open 20-day first-touch direction'
             AND experiment.tick_size = 0.00005
             AND experiment.tick_value = 6.25
             AND experiment.code_commit = campaign.code_commit
             AND experiment.config_sha256 =
                 '8378983f7db68b443d385b7cc646f0294391293ccd1873dbc3a2458ad1384c49'
             AND systematic_fx.canonical_jsonb_sha256(jsonb_build_object(
                     'cost_assumptions', experiment.cost_assumptions,
                     'execution_assumptions', experiment.execution_assumptions,
                     'feature_versions', experiment.feature_definition_versions,
                     'search_boundary', experiment.search_boundary
                 )) =
                 '8378983f7db68b443d385b7cc646f0294391293ccd1873dbc3a2458ad1384c49'
             AND (
                 SELECT count(*)
                 FROM systematic_fx.experiments AS campaign_experiment
                 WHERE campaign_experiment.campaign_id = campaign.campaign_id
             ) = 1
             AND (
                 SELECT count(*)
                 FROM systematic_fx.research_run_specs AS run_spec
                 WHERE run_spec.campaign_id = campaign.campaign_id
             ) = 12
             AND systematic_fx.bar_state_preregistration_is_exact(
                 campaign.campaign_key, experiment.experiment_id
             )
       );
$$;

CREATE FUNCTION systematic_fx.bar_state_v2b_predecessor_is_clean()
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
        ) AS profile ON profile.profile_version = 'V2A'
        WHERE campaign.campaign_key = 'bar_state_conditional_v2a'
          AND campaign.name = profile.campaign_name
          AND campaign.status = 'FROZEN'
          AND campaign.frozen_at IS NOT NULL
          AND campaign.selected_start_date = DATE '2022-01-03'
          AND campaign.selected_end_date = DATE '2026-07-31'
          AND campaign.roll_cutoff_date IS NULL
          AND campaign.holdout_revealed_at IS NULL
          AND campaign.closed_at IS NULL
          AND campaign.code_commit =
              '8688c7efb298f9644ee3821ce575349c446c6998'
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
          AND systematic_fx.canonical_jsonb_sha256(campaign.split_policy) =
              '5da1027fb2003c521b4be2eee0d2bf1238e4784467f43f7d9b9ac978223f5552'
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
          AND experiment.pattern_id IS NULL
          AND experiment.parent_experiment_id IS NULL
          AND experiment.hypothesis =
              'Completed candle state predicts next-open 20-day first-touch direction'
          AND experiment.primary_family = 'CONDITIONAL_BAR_STATE_MODEL'
          AND experiment.model_family = 'ELASTIC_NET_MULTINOMIAL_LOGISTIC'
          AND experiment.direction = 'BOTH'
          AND experiment.tick_size = 0.00005
          AND experiment.tick_value = 6.25
          AND experiment.trial_budget = 12
          AND experiment.trials_registered = 12
          AND experiment.code_commit = campaign.code_commit
          AND experiment.config_sha256 =
              'ae3ab3f4e0a77e4e0ddf83d0bca969514f94734f0009ec85deb4cf573a490769'
          AND systematic_fx.canonical_jsonb_sha256(jsonb_build_object(
                  'cost_assumptions', experiment.cost_assumptions,
                  'execution_assumptions', experiment.execution_assumptions,
                  'feature_versions', experiment.feature_definition_versions,
                  'search_boundary', experiment.search_boundary
              )) =
              'ae3ab3f4e0a77e4e0ddf83d0bca969514f94734f0009ec85deb4cf573a490769'
          AND (
              SELECT count(*)
              FROM systematic_fx.experiments AS campaign_experiment
              WHERE campaign_experiment.campaign_id = campaign.campaign_id
          ) = 1
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
              FROM systematic_fx.research_run_specs AS run_spec
              WHERE run_spec.campaign_id = predecessor.campaign_id
          ) = 12
          AND systematic_fx.bar_state_preregistration_is_exact(
              'bar_state_conditional_v2a', predecessor.experiment_id
          )
          AND (
            SELECT count(*)
            FROM systematic_fx.research_run_attempts AS attempt
            JOIN systematic_fx.research_run_specs AS run_spec
              ON run_spec.research_run_spec_id = attempt.research_run_spec_id
            WHERE run_spec.campaign_id = predecessor.campaign_id
          ) = 24
          AND (
            SELECT count(*)
            FROM systematic_fx.research_run_attempts AS attempt
            JOIN systematic_fx.research_run_specs AS run_spec
              ON run_spec.research_run_spec_id = attempt.research_run_spec_id
            WHERE run_spec.campaign_id = predecessor.campaign_id
              AND attempt.status = 'FAILED'
              AND attempt.attempt_number IN (1, 2)
              AND attempt.result_artifact_id IS NULL
              AND attempt.trade_ledger_artifact_id IS NULL
              AND attempt.reused_attempt_id IS NULL
              AND attempt.started_at IS NOT NULL
              AND attempt.finished_at IS NOT NULL
              AND btrim(COALESCE(attempt.error_message, '')) <> ''
              AND attempt.result_summary = jsonb_build_object(
                  'candidate_key',
                      run_spec.canonical_spec #>>
                          '{parameters,bar_state_candidate_key}',
                  'run_fingerprint', run_spec.run_fingerprint
              )
          ) = 24
          AND (
            SELECT count(*)
            FROM (
                SELECT attempt.research_run_spec_id
                FROM systematic_fx.research_run_attempts AS attempt
                JOIN systematic_fx.research_run_specs AS run_spec
                  ON run_spec.research_run_spec_id = attempt.research_run_spec_id
                WHERE run_spec.campaign_id = predecessor.campaign_id
                GROUP BY attempt.research_run_spec_id
                HAVING count(*) = 2
                   AND count(*) FILTER (WHERE attempt.attempt_number = 1) = 1
                   AND count(*) FILTER (WHERE attempt.attempt_number = 2) = 1
            ) AS exact_attempt_pairs
          ) = 12
          AND NOT EXISTS (
            SELECT 1
            FROM systematic_fx.bar_state_artifact_links AS link
            WHERE link.campaign_id = predecessor.campaign_id
          )
    );
$$;

CREATE FUNCTION systematic_fx.enforce_bar_state_v2b_predecessor_campaign()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    dataset_record systematic_fx.datasets%ROWTYPE;
BEGIN
    IF NEW.campaign_key = 'bar_state_conditional_v2b' THEN
        SELECT *
        INTO dataset_record
        FROM systematic_fx.datasets AS dataset
        WHERE dataset.dataset_id = NEW.dataset_id;
        IF NOT FOUND
           OR NEW.name IS DISTINCT FROM
                'Frozen conditional candle-state Discovery v2b'
           OR NEW.status IS DISTINCT FROM 'FROZEN'
           OR NEW.selected_start_date IS DISTINCT FROM DATE '2022-01-03'
           OR NEW.selected_end_date IS DISTINCT FROM DATE '2026-07-31'
           OR NEW.roll_cutoff_date IS NOT NULL
           OR NEW.data_manifest_sha256 IS DISTINCT FROM
                'e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc'
           OR NEW.feature_version IS DISTINCT FROM 'bar_state_features_v1'
           OR NEW.outcome_version IS DISTINCT FROM
                'bar_state_twenty_day_first_touch_labels_v1'
           OR NEW.cost_model_version IS DISTINCT FROM 'BAR_TRADE_ONLY_COSTS_V1'
           OR NEW.execution_model_version IS DISTINCT FROM
                'bar_state_next_open_49_cell_replay_v1'
           OR NEW.code_commit !~ '^([0-9a-f]{40}|[0-9a-f]{64})$'
           OR NEW.config_sha256 IS DISTINCT FROM
                'cee6838d9c85498818140bd02ae92483fe17c080d4909190eb0b83f790e5bb60'
           OR systematic_fx.canonical_jsonb_sha256(NEW.split_policy)
                IS DISTINCT FROM
                '5da1027fb2003c521b4be2eee0d2bf1238e4784467f43f7d9b9ac978223f5552'
           OR NEW.trial_budget IS DISTINCT FROM 12
           OR NEW.finalist_budget IS DISTINCT FROM 4
           OR NEW.frozen_at IS NULL
           OR NEW.holdout_revealed_at IS NOT NULL
           OR NEW.closed_at IS NOT NULL
           OR dataset_record.dataset_key IS DISTINCT FROM
                'glbx_mdp3_mbp_10_6e_fut_v1'
           OR dataset_record.manifest_sha256 IS DISTINCT FROM
                '14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de'
           OR dataset_record.status IN ('REJECTED', 'RETIRED') THEN
            RAISE EXCEPTION 'State V2B campaign identity is not exact';
        END IF;
        PERFORM campaign.campaign_id
        FROM systematic_fx.campaigns AS campaign
        WHERE campaign.campaign_key = 'bar_state_conditional_v2a'
        FOR UPDATE;
        IF NOT FOUND
           OR systematic_fx.bar_state_v2b_predecessor_is_clean()
                IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'State V2B requires its exact clean failed V2A predecessor';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER campaigns_require_bar_state_v2b_predecessor
BEFORE INSERT ON systematic_fx.campaigns
FOR EACH ROW EXECUTE FUNCTION systematic_fx.enforce_bar_state_v2b_predecessor_campaign();

CREATE FUNCTION systematic_fx.bar_state_registered_successor_key(
    predecessor_campaign_key text
)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog
AS $$
    SELECT CASE predecessor_campaign_key
        WHEN 'bar_state_conditional_v2' THEN 'bar_state_conditional_v2a'
        WHEN 'bar_state_conditional_v2a' THEN 'bar_state_conditional_v2b'
        ELSE NULL
    END;
$$;

CREATE FUNCTION systematic_fx.enforce_bar_state_predecessor_artifact_freeze()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    predecessor_key text;
    successor_key text;
    predecessor_registered boolean;
BEGIN
    SELECT profile.campaign_key
    INTO predecessor_key
    FROM (
        VALUES
            ('bar_state_conditional_v2'::text,
             'bar_state_conditional_v2'::text),
            ('bar_state_conditional_v2a'::text,
             'bar_state_conditional_v2a'::text),
            ('bar_state_conditional_v2b'::text,
             'bar_state_conditional_v2b'::text)
    ) AS profile(campaign_key, artifact_type)
    WHERE profile.artifact_type = NEW.artifact_type;
    successor_key := systematic_fx.bar_state_registered_successor_key(
        predecessor_key
    );
    IF predecessor_key IS NULL OR successor_key IS NULL THEN
        RETURN NEW;
    END IF;
    PERFORM campaign.campaign_id
    FROM systematic_fx.campaigns AS campaign
    WHERE campaign.campaign_key = predecessor_key
    FOR UPDATE;
    predecessor_registered := FOUND;
    IF EXISTS (
        SELECT 1
        FROM systematic_fx.artifacts AS existing
        WHERE existing.artifact_key = NEW.artifact_key
          AND existing.artifact_type = NEW.artifact_type
          AND existing.uri = NEW.uri
          AND existing.sha256 = NEW.sha256
          AND existing.byte_size = NEW.byte_size
          AND existing.media_type IS NOT DISTINCT FROM NEW.media_type
          AND existing.producer_job_id IS NOT DISTINCT FROM NEW.producer_job_id
          AND existing.metadata = NEW.metadata
    ) THEN
        RETURN NEW;
    END IF;
    IF predecessor_registered AND EXISTS (
        SELECT 1
        FROM systematic_fx.campaigns AS successor
        WHERE successor.campaign_key = successor_key
    ) THEN
        RAISE EXCEPTION 'State predecessor artifacts are frozen after successor registration';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER aa_artifacts_freeze_bar_state_predecessor
BEFORE INSERT ON systematic_fx.artifacts
FOR EACH ROW EXECUTE FUNCTION systematic_fx.enforce_bar_state_predecessor_artifact_freeze();

CREATE FUNCTION systematic_fx.enforce_bar_state_predecessor_experiment_freeze()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    target_campaign_key text;
    target_campaign_code_commit text;
    target_profile_version text;
    target_experiment_key text;
    target_artifact_type text;
    target_config_file_sha256 text;
    target_config_semantic_sha256 text;
    target_candidate_catalog_sha256 text;
    target_campaign_definition_sha256 text;
    expected_experiment_config_sha256 text;
    successor_key text;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        PERFORM campaign.campaign_id
        FROM systematic_fx.campaigns AS campaign
        WHERE campaign.campaign_id IN (OLD.campaign_id, NEW.campaign_id)
          AND EXISTS (
              SELECT 1
              FROM systematic_fx.bar_state_governance_profile(
                  campaign.campaign_key
              )
          )
        ORDER BY campaign.campaign_key
        FOR UPDATE;
        IF FOUND THEN
            RAISE EXCEPTION 'State campaign experiment identity is immutable';
        END IF;
        RETURN NEW;
    END IF;
    SELECT campaign.campaign_key, campaign.code_commit,
           profile.profile_version, profile.experiment_key,
           profile.artifact_type, profile.config_file_sha256,
           profile.config_semantic_sha256, profile.candidate_catalog_sha256,
           profile.campaign_definition_sha256
    INTO target_campaign_key, target_campaign_code_commit,
         target_profile_version, target_experiment_key, target_artifact_type,
         target_config_file_sha256, target_config_semantic_sha256,
         target_candidate_catalog_sha256, target_campaign_definition_sha256
    FROM systematic_fx.campaigns AS campaign
    JOIN LATERAL systematic_fx.bar_state_governance_profile(
        campaign.campaign_key
    ) AS profile ON true
    WHERE campaign.campaign_id = NEW.campaign_id;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;
    PERFORM campaign.campaign_id
    FROM systematic_fx.campaigns AS campaign
    WHERE campaign.campaign_id = NEW.campaign_id
    FOR UPDATE;
    IF EXISTS (
        SELECT 1
        FROM systematic_fx.experiments AS existing
        WHERE existing.experiment_key = NEW.experiment_key
    ) THEN
        RETURN NEW;
    END IF;
    successor_key := systematic_fx.bar_state_registered_successor_key(
        target_campaign_key
    );
    IF successor_key IS NOT NULL AND EXISTS (
        SELECT 1
        FROM systematic_fx.campaigns AS successor
        WHERE successor.campaign_key = successor_key
    ) THEN
        RAISE EXCEPTION 'State predecessor experiment catalog is frozen after successor registration';
    END IF;
    expected_experiment_config_sha256 := CASE target_profile_version
        WHEN 'V2' THEN
            '8378983f7db68b443d385b7cc646f0294391293ccd1873dbc3a2458ad1384c49'
        WHEN 'V2A' THEN
            'ae3ab3f4e0a77e4e0ddf83d0bca969514f94734f0009ec85deb4cf573a490769'
        WHEN 'V2B' THEN
            'ae3ab3f4e0a77e4e0ddf83d0bca969514f94734f0009ec85deb4cf573a490769'
    END;
    IF EXISTS (
           SELECT 1
           FROM systematic_fx.experiments AS existing
           WHERE existing.campaign_id = NEW.campaign_id
       )
       OR NEW.experiment_key IS DISTINCT FROM target_experiment_key
       OR NEW.pattern_id IS NOT NULL
       OR NEW.parent_experiment_id IS NOT NULL
       OR NEW.primary_family IS DISTINCT FROM 'CONDITIONAL_BAR_STATE_MODEL'
       OR NEW.status IS DISTINCT FROM 'FROZEN'
       OR NEW.hypothesis IS DISTINCT FROM
            'Completed candle state predicts next-open 20-day first-touch direction'
       OR NEW.direction IS DISTINCT FROM 'BOTH'
       OR NEW.model_family IS DISTINCT FROM
            'ELASTIC_NET_MULTINOMIAL_LOGISTIC'
       OR NEW.tick_size IS DISTINCT FROM 0.00005::numeric
       OR NEW.tick_value IS DISTINCT FROM 6.25::numeric
       OR NEW.trial_budget IS DISTINCT FROM 12
       OR NEW.trials_registered IS DISTINCT FROM 12
       OR NEW.code_commit IS DISTINCT FROM target_campaign_code_commit
       OR NEW.config_sha256 IS DISTINCT FROM expected_experiment_config_sha256
       OR systematic_fx.canonical_jsonb_sha256(
            jsonb_build_object(
                'cost_assumptions', NEW.cost_assumptions,
                'execution_assumptions', NEW.execution_assumptions,
                'feature_versions', NEW.feature_definition_versions,
                'search_boundary', NEW.search_boundary
            )
          ) IS DISTINCT FROM expected_experiment_config_sha256
       OR NEW.registration_artifact_id IS NULL
       OR NEW.frozen_at IS NULL
       OR NEW.completed_at IS NOT NULL
       OR NOT EXISTS (
            SELECT 1
            FROM systematic_fx.artifacts AS registration
            WHERE registration.artifact_id = NEW.registration_artifact_id
              AND registration.artifact_type = target_artifact_type
              AND registration.artifact_key =
                  target_artifact_type || ':registration:' ||
                      target_campaign_definition_sha256
              AND registration.media_type = 'application/json'
              AND registration.sha256 =
                  registration.metadata #>> '{content_sha256}'
              AND registration.artifact_key =
                  registration.metadata #>> '{artifact_key}'
              AND registration.metadata #>> '{artifact_schema}' =
                  'systematic_fx.bar_state_registration_artifact.v1'
              AND registration.metadata #>>
                      '{logical_identity,artifact_kind}' = 'REGISTRATION'
              AND registration.metadata #>>
                      '{logical_identity,campaign_key}' = target_campaign_key
              AND registration.metadata #>>
                      '{logical_identity,campaign_definition_sha256}' =
                  target_campaign_definition_sha256
              AND registration.metadata #>>
                      '{logical_identity,candidate_catalog_sha256}' =
                  target_candidate_catalog_sha256
              AND registration.metadata #>>
                      '{logical_identity,lineage,config_file_sha256}' =
                  target_config_file_sha256
              AND registration.metadata #>>
                      '{logical_identity,lineage,config_semantic_sha256}' =
                  target_config_semantic_sha256
              AND registration.metadata #>>
                      '{logical_identity,lineage,candidate_catalog_sha256}' =
                  target_candidate_catalog_sha256
              AND registration.metadata #>> '{artifact_identity_sha256}' =
                  systematic_fx.canonical_jsonb_sha256(
                      registration.metadata - 'artifact_identity_sha256' -
                          'content_sha256'
                  )
       ) THEN
        RAISE EXCEPTION 'State campaign requires its one exact frozen experiment';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER aa_experiments_freeze_bar_state_predecessor
BEFORE INSERT OR UPDATE ON systematic_fx.experiments
FOR EACH ROW EXECUTE FUNCTION systematic_fx.enforce_bar_state_predecessor_experiment_freeze();

CREATE OR REPLACE FUNCTION systematic_fx.enforce_bar_state_v2_predecessor_runspec_freeze()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    predecessor_key text;
    successor_key text;
    expected_experiment_key text;
    expected_engine_version text;
    candidate_key text;
BEGIN
    SELECT campaign.campaign_key, profile.experiment_key,
           profile.engine_version
    INTO predecessor_key, expected_experiment_key, expected_engine_version
    FROM systematic_fx.campaigns AS campaign
    JOIN LATERAL systematic_fx.bar_state_governance_profile(
        campaign.campaign_key
    ) AS profile ON true
    WHERE campaign.campaign_id = NEW.campaign_id;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;
    PERFORM campaign.campaign_id
    FROM systematic_fx.campaigns AS campaign
    WHERE campaign.campaign_id = NEW.campaign_id
    FOR UPDATE;
    successor_key := systematic_fx.bar_state_registered_successor_key(
        predecessor_key
    );
    IF successor_key IS NOT NULL AND EXISTS (
        SELECT 1
        FROM systematic_fx.campaigns AS successor
        WHERE successor.campaign_key = successor_key
    ) THEN
        RAISE EXCEPTION 'State predecessor RunSpec catalog is frozen after successor registration';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM systematic_fx.research_run_specs AS existing
        WHERE existing.run_fingerprint = NEW.run_fingerprint
    ) THEN
        RETURN NEW;
    END IF;
    candidate_key := NEW.canonical_spec #>>
        '{parameters,bar_state_candidate_key}';
    IF NEW.experiment_id IS NULL
       OR NOT systematic_fx.bar_state_preregistration_is_exact(
            predecessor_key, NEW.experiment_id
       )
       OR NOT EXISTS (
            SELECT 1
            FROM systematic_fx.experiments AS experiment
            WHERE experiment.experiment_id = NEW.experiment_id
              AND experiment.campaign_id = NEW.campaign_id
              AND experiment.experiment_key = expected_experiment_key
       )
       OR NEW.run_kind IS DISTINCT FROM 'MODEL_FIT'
       OR NEW.engine_version IS DISTINCT FROM expected_engine_version
       OR candidate_key !~
            '^bsv2_tf(0300|1800)_fs(morphology|state)_cm(005|010|015)$'
       OR NOT EXISTS (
            SELECT 1
            FROM systematic_fx.experiment_trials AS trial
            WHERE trial.experiment_id = NEW.experiment_id
              AND trial.trial_key = candidate_key
              AND trial.status = 'REGISTERED'
              AND trial.research_run_spec_id IS NULL
              AND trial.parameters #>> '{candidate_definition_sha256}' =
                  NEW.canonical_spec #>>
                      '{parameters,bar_state_candidate_definition_sha256}'
              AND trial.parameters_sha256 = NEW.canonical_spec #>>
                      '{parameters,bar_state_trial_parameters_sha256}'
       )
       OR EXISTS (
            SELECT 1
            FROM systematic_fx.research_run_specs AS existing
            WHERE existing.campaign_id = NEW.campaign_id
              AND existing.canonical_spec #>>
                      '{parameters,bar_state_candidate_key}' = candidate_key
       )
       OR (
            SELECT count(*)
            FROM systematic_fx.research_run_specs AS existing
            WHERE existing.campaign_id = NEW.campaign_id
       ) >= 12 THEN
        RAISE EXCEPTION 'State campaign RunSpec catalog accepts only its exact 12 candidates';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION systematic_fx.enforce_bar_state_predecessor_trial_freeze()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    target_experiment_id bigint;
    predecessor_key text;
    successor_key text;
BEGIN
    target_experiment_id := CASE
        WHEN TG_OP = 'INSERT' THEN NEW.experiment_id
        ELSE OLD.experiment_id
    END;
    SELECT campaign.campaign_key
    INTO predecessor_key
    FROM systematic_fx.experiments AS experiment
    JOIN systematic_fx.campaigns AS campaign
      ON campaign.campaign_id = experiment.campaign_id
    WHERE experiment.experiment_id = target_experiment_id;
    successor_key := systematic_fx.bar_state_registered_successor_key(
        predecessor_key
    );
    IF successor_key IS NULL THEN
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END IF;
    PERFORM campaign.campaign_id
    FROM systematic_fx.campaigns AS campaign
    WHERE campaign.campaign_key = predecessor_key
    FOR UPDATE;
    IF EXISTS (
        SELECT 1
        FROM systematic_fx.campaigns AS successor
        WHERE successor.campaign_key = successor_key
    ) THEN
        RAISE EXCEPTION 'State predecessor lifecycle is frozen after successor registration';
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE TRIGGER aa_experiment_trials_freeze_bar_state_predecessor
BEFORE INSERT OR UPDATE OR DELETE ON systematic_fx.experiment_trials
FOR EACH ROW EXECUTE FUNCTION systematic_fx.enforce_bar_state_predecessor_trial_freeze();

CREATE FUNCTION systematic_fx.enforce_bar_state_predecessor_artifact_link_freeze()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    predecessor_key text;
    successor_key text;
BEGIN
    SELECT campaign.campaign_key
    INTO predecessor_key
    FROM systematic_fx.campaigns AS campaign
    WHERE campaign.campaign_id = NEW.campaign_id;
    successor_key := systematic_fx.bar_state_registered_successor_key(
        predecessor_key
    );
    IF successor_key IS NULL THEN
        RETURN NEW;
    END IF;
    PERFORM campaign.campaign_id
    FROM systematic_fx.campaigns AS campaign
    WHERE campaign.campaign_key = predecessor_key
    FOR UPDATE;
    IF EXISTS (
        SELECT 1
        FROM systematic_fx.campaigns AS successor
        WHERE successor.campaign_key = successor_key
    ) THEN
        RAISE EXCEPTION 'State predecessor artifact links are frozen after successor registration';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER aa_bar_state_artifact_links_freeze_predecessor
BEFORE INSERT ON systematic_fx.bar_state_artifact_links
FOR EACH ROW EXECUTE FUNCTION systematic_fx.enforce_bar_state_predecessor_artifact_link_freeze();

CREATE FUNCTION systematic_fx.enforce_bar_state_v2b_feature_schema_link()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    target_campaign_key text;
    target_schema_sha256 text;
BEGIN
    IF NEW.artifact_role IS DISTINCT FROM 'FEATURE' THEN
        RETURN NEW;
    END IF;
    SELECT campaign.campaign_key,
           artifact.metadata #>> '{schema_sha256}'
    INTO target_campaign_key, target_schema_sha256
    FROM systematic_fx.campaigns AS campaign
    JOIN systematic_fx.artifacts AS artifact
      ON artifact.artifact_id = NEW.artifact_id
    WHERE campaign.campaign_id = NEW.campaign_id;
    IF target_campaign_key = 'bar_state_conditional_v2b'
       AND target_schema_sha256 IS DISTINCT FROM
            'da7e500759276e85483f070451595eb083f3c15e76541bc2a2bd86c6483ebef3' THEN
        RAISE EXCEPTION 'State V2B FEATURE requires its exact round-trip Arrow schema';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER bar_state_artifact_links_enforce_v2b_feature_schema
BEFORE INSERT ON systematic_fx.bar_state_artifact_links
FOR EACH ROW EXECUTE FUNCTION systematic_fx.enforce_bar_state_v2b_feature_schema_link();

CREATE FUNCTION systematic_fx.enforce_bar_state_v2a_predecessor_attempt_freeze()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    target_spec_id bigint;
    predecessor_campaign_id bigint;
BEGIN
    target_spec_id := CASE
        WHEN TG_OP = 'INSERT' THEN NEW.research_run_spec_id
        ELSE OLD.research_run_spec_id
    END;
    SELECT campaign.campaign_id
    INTO predecessor_campaign_id
    FROM systematic_fx.research_run_specs AS run_spec
    JOIN systematic_fx.campaigns AS campaign
      ON campaign.campaign_id = run_spec.campaign_id
    WHERE run_spec.research_run_spec_id = target_spec_id
      AND campaign.campaign_key = 'bar_state_conditional_v2a';
    IF predecessor_campaign_id IS NULL THEN
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END IF;
    PERFORM campaign.campaign_id
    FROM systematic_fx.campaigns AS campaign
    WHERE campaign.campaign_id = predecessor_campaign_id
    FOR UPDATE;
    IF EXISTS (
        SELECT 1
        FROM systematic_fx.campaigns AS successor
        WHERE successor.campaign_key = 'bar_state_conditional_v2b'
    ) THEN
        RAISE EXCEPTION 'State V2A predecessor attempt lifecycle is frozen after V2B registration';
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE TRIGGER aa_research_run_attempts_freeze_bar_state_v2a_predecessor
BEFORE INSERT OR UPDATE OR DELETE ON systematic_fx.research_run_attempts
FOR EACH ROW EXECUTE FUNCTION systematic_fx.enforce_bar_state_v2a_predecessor_attempt_freeze();

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
            profile_version IN ('V2A', 'V2B')
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

COMMENT ON FUNCTION systematic_fx.bar_state_governance_profile(text) IS
    'Immutable three-profile identity lookup for governed State V2, V2A, and V2B campaigns.';
COMMENT ON FUNCTION systematic_fx.bar_state_v2b_predecessor_is_clean() IS
    'Fails closed unless exact State V2A has two clean FAILED attempts per candidate and no governed evidence.';
COMMENT ON FUNCTION systematic_fx.bar_state_registered_successor_key(text) IS
    'Immutable immediate-successor map used to serialize amendment registration and predecessor freezes.';

INSERT INTO systematic_fx.publication_outbox (
    scope_key, request_version, delivered_version, requested_at
)
VALUES ('public-research', 1, 0, statement_timestamp())
ON CONFLICT (scope_key) DO UPDATE
SET request_version = systematic_fx.publication_outbox.request_version + 1,
    requested_at = statement_timestamp(),
    last_error = NULL;

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (27, 'bar_state_v2b_parquet_schema_amendment', :'migration_checksum');

COMMIT;
