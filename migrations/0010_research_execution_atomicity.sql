BEGIN;

CREATE UNIQUE INDEX research_run_attempts_one_active
    ON systematic_fx.research_run_attempts (research_run_spec_id)
    WHERE status IN ('QUEUED', 'RUNNING');

CREATE FUNCTION systematic_fx.require_duplicate_skip_success()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    reused_status text;
    reused_spec_id bigint;
BEGIN
    IF NEW.status <> 'SKIPPED_DUPLICATE' THEN
        RETURN NEW;
    END IF;

    SELECT status, research_run_spec_id
    INTO reused_status, reused_spec_id
    FROM systematic_fx.research_run_attempts
    WHERE research_run_attempt_id = NEW.reused_attempt_id;

    IF reused_status IS DISTINCT FROM 'SUCCEEDED'
       OR reused_spec_id IS DISTINCT FROM NEW.research_run_spec_id THEN
        RAISE EXCEPTION
            'SKIPPED_DUPLICATE must reuse a SUCCEEDED attempt for the same RunSpec';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER research_run_attempts_require_duplicate_success
BEFORE INSERT OR UPDATE ON systematic_fx.research_run_attempts
FOR EACH ROW EXECUTE FUNCTION systematic_fx.require_duplicate_skip_success();

CREATE FUNCTION systematic_fx.require_phase1a_exposure_success()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    campaign_key_value text;
    matching_success_count integer;
BEGIN
    SELECT campaign_key
    INTO STRICT campaign_key_value
    FROM systematic_fx.campaigns
    WHERE campaign_id = NEW.campaign_id;

    IF campaign_key_value <> 'phase1a_conservative_screening_v1' THEN
        RETURN NEW;
    END IF;
    IF NEW.visible_to_ai IS DISTINCT FROM true
       OR NEW.research_eligible IS DISTINCT FROM false THEN
        RAISE EXCEPTION
            'Phase 1A Discovery exposure must be AI-visible and screening-only';
    END IF;
    IF NEW.research_run_spec_id IS NULL OR NEW.result_artifact_id IS NULL THEN
        RAISE EXCEPTION
            'Phase 1A Discovery exposure requires a RunSpec and result artifact';
    END IF;

    SELECT count(*)::integer
    INTO matching_success_count
    FROM systematic_fx.research_run_attempts
    WHERE research_run_spec_id = NEW.research_run_spec_id
      AND status = 'SUCCEEDED'
      AND result_artifact_id = NEW.result_artifact_id;

    IF matching_success_count <> 1 THEN
        RAISE EXCEPTION
            'Phase 1A Discovery exposure requires exactly one matching SUCCEEDED attempt';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER discovery_exposures_require_phase1a_success
BEFORE INSERT OR UPDATE ON systematic_fx.discovery_exposures
FOR EACH ROW EXECUTE FUNCTION systematic_fx.require_phase1a_exposure_success();

COMMENT ON INDEX systematic_fx.research_run_attempts_one_active IS
    'At most one QUEUED or RUNNING executor may own a canonical RunSpec.';
COMMENT ON TRIGGER research_run_attempts_require_duplicate_success
    ON systematic_fx.research_run_attempts IS
    'A duplicate skip can reference only the immutable success for the same RunSpec.';
COMMENT ON TRIGGER discovery_exposures_require_phase1a_success
    ON systematic_fx.discovery_exposures IS
    'Prevents an append-preserved Phase 1A exposure from becoming visible before its '
    'matching result artifact and successful attempt commit in the same transaction.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (10, 'research_execution_atomicity', :'migration_checksum');

COMMIT;
