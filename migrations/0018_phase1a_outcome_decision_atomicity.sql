BEGIN;

CREATE FUNCTION systematic_fx.require_phase1a_outcome_screening_decisions()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    observed_count integer;
    observed_directions integer;
BEGIN
    IF NEW.status <> 'SUCCEEDED' THEN
        RETURN NULL;
    END IF;
    SELECT count(*)::integer, count(DISTINCT direction)::integer
    INTO observed_count, observed_directions
    FROM systematic_fx.phase1a_outcome_screening_decisions
    WHERE outcome_replay_manifest_id = NEW.outcome_replay_manifest_id;
    IF observed_count <> 2 OR observed_directions <> 2 THEN
        RAISE EXCEPTION
            'successful Phase 1A outcome replay requires atomic LONG and SHORT screening decisions';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER phase1a_outcome_success_requires_decisions
AFTER INSERT OR UPDATE
ON systematic_fx.phase1a_outcome_replay_manifests
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION
    systematic_fx.require_phase1a_outcome_screening_decisions();

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (18, 'phase1a_outcome_decision_atomicity', :'migration_checksum');

COMMIT;
