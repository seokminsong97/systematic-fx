BEGIN;

CREATE OR REPLACE FUNCTION systematic_fx.phase1a_artifact_is_protected(
    target_artifact_id bigint
)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
    SELECT
        EXISTS (
            SELECT 1
            FROM systematic_fx.artifacts AS artifact
            WHERE artifact.artifact_id = target_artifact_id
              AND (
                  artifact.artifact_type = 'PHASE1A_FEATURE_BUILD_MANIFEST'
                  OR (
                      artifact.artifact_type IN (
                          'PHASE1A_ELIGIBLE_CALENDAR',
                          'PHASE1A_CAMPAIGN_SPLIT',
                          'PHASE1A_CODE_SNAPSHOT',
                          'PHASE1A_SCREENING_REGISTRY'
                      )
                      AND artifact.metadata ->> 'campaign_key' =
                          'phase1a_conservative_screening_v1'
                  )
              )
        )
        OR EXISTS (
            SELECT 1
            FROM systematic_fx.discovery_exposures AS exposure
            JOIN systematic_fx.campaigns AS campaign
              ON campaign.campaign_id = exposure.campaign_id
            WHERE exposure.result_artifact_id = target_artifact_id
              AND campaign.campaign_key = 'phase1a_conservative_screening_v1'
        )
        OR EXISTS (
            SELECT 1
            FROM systematic_fx.research_run_attempts AS attempt
            JOIN systematic_fx.research_run_specs AS run_spec
              ON run_spec.research_run_spec_id = attempt.research_run_spec_id
            JOIN systematic_fx.campaigns AS campaign
              ON campaign.campaign_id = run_spec.campaign_id
            WHERE (attempt.result_artifact_id = target_artifact_id
                   OR attempt.trade_ledger_artifact_id = target_artifact_id)
              AND campaign.campaign_key = 'phase1a_conservative_screening_v1'
        )
        OR EXISTS (
            SELECT 1
            FROM systematic_fx.pattern_ledger AS pattern
            JOIN systematic_fx.campaigns AS campaign
              ON campaign.campaign_id = pattern.campaign_id
            WHERE pattern.context_artifact_id = target_artifact_id
              AND campaign.campaign_key = 'phase1a_conservative_screening_v1'
        )
        OR EXISTS (
            SELECT 1
            FROM systematic_fx.experiments AS experiment
            JOIN systematic_fx.campaigns AS campaign
              ON campaign.campaign_id = experiment.campaign_id
            WHERE experiment.registration_artifact_id = target_artifact_id
              AND campaign.campaign_key = 'phase1a_conservative_screening_v1'
        );
$$;

COMMENT ON FUNCTION systematic_fx.phase1a_artifact_is_protected(bigint) IS
    'Protects Phase 1A control/provenance inputs and every artifact owned by a '
    'governed result, attempt, pattern, feature build, or experiment registration.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (11, 'phase1a_control_artifact_immutability', :'migration_checksum');

COMMIT;
