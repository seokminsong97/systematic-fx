BEGIN;

CREATE TRIGGER research_run_specs_publication_refresh
AFTER INSERT OR UPDATE OR DELETE
ON systematic_fx.research_run_specs
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();

CREATE TRIGGER research_run_attempts_publication_refresh
AFTER INSERT OR UPDATE OR DELETE
ON systematic_fx.research_run_attempts
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();

CREATE TRIGGER phase1a_outcome_checkpoints_publication_refresh
AFTER INSERT OR UPDATE OR DELETE
ON systematic_fx.phase1a_outcome_replay_checkpoints
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();

INSERT INTO systematic_fx.publication_outbox (
    scope_key, request_version, delivered_version, requested_at
)
VALUES ('public-research', 1, 0, statement_timestamp())
ON CONFLICT (scope_key) DO UPDATE
SET request_version = systematic_fx.publication_outbox.request_version + 1,
    requested_at = statement_timestamp(),
    last_error = NULL;

COMMENT ON TRIGGER research_run_attempts_publication_refresh
ON systematic_fx.research_run_attempts IS
    'Refresh the public projection when governed run execution state changes.';
COMMENT ON TRIGGER phase1a_outcome_checkpoints_publication_refresh
ON systematic_fx.phase1a_outcome_replay_checkpoints IS
    'Refresh the public projection when chronological outcome replay progress advances.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (20, 'publication_run_progress', :'migration_checksum');

COMMIT;
