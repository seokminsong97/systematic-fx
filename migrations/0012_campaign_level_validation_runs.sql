BEGIN;

ALTER TABLE systematic_fx.research_run_specs
    DROP CONSTRAINT research_run_specs_experiment_ownership;

ALTER TABLE systematic_fx.research_run_specs
    ADD CONSTRAINT research_run_specs_experiment_ownership
        CHECK (experiment_id IS NOT NULL
               OR run_kind IN (
                   'FEATURE_BUILD',
                   'OUTCOME_BUILD',
                   'AI_SLICE',
                   'QUERY',
                   'VALIDATION'
               ));

COMMENT ON CONSTRAINT research_run_specs_experiment_ownership
    ON systematic_fx.research_run_specs IS
    'Common data builds, AI exposures, query projections, and validation/control '
    'runs may belong to the campaign; strategy and performance runs must name '
    'the exact owning experiment.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (12, 'campaign_level_validation_runs', :'migration_checksum');

COMMIT;
