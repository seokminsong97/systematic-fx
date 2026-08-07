BEGIN;

CREATE FUNCTION systematic_fx.protect_phase1a_pattern_rollup()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    campaign_key_value text;
BEGIN
    IF TG_OP = 'DELETE' THEN
        SELECT campaign_key INTO STRICT campaign_key_value
        FROM systematic_fx.campaigns
        WHERE campaign_id = OLD.campaign_id;
        IF campaign_key_value = 'phase1a_conservative_screening_v1' THEN
            RAISE EXCEPTION 'Phase 1A pattern roll-ups cannot be deleted';
        END IF;
        RETURN OLD;
    END IF;

    SELECT campaign_key INTO STRICT campaign_key_value
    FROM systematic_fx.campaigns
    WHERE campaign_id = NEW.campaign_id;
    IF campaign_key_value <> 'phase1a_conservative_screening_v1' THEN
        RETURN NEW;
    END IF;

    IF NEW.context_artifact_id IS NULL THEN
        RAISE EXCEPTION 'Phase 1A pattern roll-ups require an immutable context artifact';
    END IF;
    IF (NEW.feature_definition_versions ->> 'rollup_schema')
           IS DISTINCT FROM 'systematic_fx.phase1a_pattern_rollup.v1'
       OR (NEW.forward_first_touch_summaries ->> 'rollup_schema')
           IS DISTINCT FROM 'systematic_fx.phase1a_pattern_rollup.v1' THEN
        RAISE EXCEPTION 'Phase 1A pattern roll-up schema is missing or invalid';
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.pattern_id IS DISTINCT FROM OLD.pattern_id
           OR NEW.pattern_key IS DISTINCT FROM OLD.pattern_key
           OR NEW.campaign_id IS DISTINCT FROM OLD.campaign_id
           OR NEW.parent_pattern_id IS DISTINCT FROM OLD.parent_pattern_id
           OR NEW.first_seen_from IS DISTINCT FROM OLD.first_seen_from
           OR NEW.first_seen_to IS DISTINCT FROM OLD.first_seen_to
           OR NEW.direction IS DISTINCT FROM OLD.direction
           OR NEW.entry_condition IS DISTINCT FROM OLD.entry_condition
           OR NEW.economic_rationale IS DISTINCT FROM OLD.economic_rationale
           OR NEW.cost_assumptions IS DISTINCT FROM OLD.cost_assumptions
           OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
            RAISE EXCEPTION 'Phase 1A pattern identity is immutable';
        END IF;
        IF NEW.support_count < OLD.support_count THEN
            RAISE EXCEPTION 'Phase 1A pattern support cannot decrease';
        END IF;
        IF NEW.last_updated_interval < OLD.last_updated_interval
           OR NEW.updated_at < OLD.updated_at THEN
            RAISE EXCEPTION 'Phase 1A pattern roll-up time cannot move backward';
        END IF;
        IF OLD.status = 'REJECTED' AND NEW.status <> OLD.status THEN
            RAISE EXCEPTION 'A rejected Phase 1A pattern is terminal';
        END IF;
        IF OLD.status = 'PROMOTED' AND NEW.status <> OLD.status THEN
            RAISE EXCEPTION 'A promoted Phase 1A pattern is terminal';
        END IF;
        IF OLD.status = 'REGISTERED' AND NEW.status = 'OPEN' THEN
            RAISE EXCEPTION 'A registered Phase 1A pattern cannot return to OPEN';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER pattern_ledger_protect_phase1a_rollup
BEFORE INSERT OR UPDATE OR DELETE ON systematic_fx.pattern_ledger
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_phase1a_pattern_rollup();

COMMENT ON TRIGGER pattern_ledger_protect_phase1a_rollup
    ON systematic_fx.pattern_ledger IS
    'Preserves Phase 1A pattern identity and monotonic roll-ups; immutable QUERY '
    'exposures, RunSpecs, and result artifacts remain the slice-level source of truth.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (8, 'phase1a_pattern_rollup_integrity', :'migration_checksum');

COMMIT;
