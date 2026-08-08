BEGIN;

ALTER TABLE systematic_fx.discovery_exposures
    ADD CONSTRAINT discovery_exposures_campaign_run_spec_fk
        FOREIGN KEY (campaign_id, research_run_spec_id)
        REFERENCES systematic_fx.research_run_specs(campaign_id, research_run_spec_id);

CREATE FUNCTION systematic_fx.require_phase1a_discovery_run_spec()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    campaign_key_value text;
    run_kind_value text;
BEGIN
    SELECT campaign_key INTO STRICT campaign_key_value
    FROM systematic_fx.campaigns
    WHERE campaign_id = NEW.campaign_id;

    IF campaign_key_value = 'phase1a_conservative_screening_v1' THEN
        IF NEW.research_run_spec_id IS NULL THEN
            RAISE EXCEPTION 'Phase 1A Discovery exposure requires research_run_spec_id';
        END IF;

        SELECT run_kind INTO STRICT run_kind_value
        FROM systematic_fx.research_run_specs
        WHERE campaign_id = NEW.campaign_id
          AND research_run_spec_id = NEW.research_run_spec_id;

        IF NEW.exposure_type = 'AI_SLICE' AND run_kind_value <> 'AI_SLICE' THEN
            RAISE EXCEPTION 'AI_SLICE exposure requires an AI_SLICE RunSpec';
        ELSIF NEW.exposure_type <> 'AI_SLICE' AND run_kind_value <> 'QUERY' THEN
            RAISE EXCEPTION 'non-AI_SLICE exposure requires a QUERY RunSpec';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER discovery_exposures_require_phase1a_run_spec
BEFORE INSERT OR UPDATE ON systematic_fx.discovery_exposures
FOR EACH ROW EXECUTE FUNCTION systematic_fx.require_phase1a_discovery_run_spec();

CREATE FUNCTION systematic_fx.protect_phase1a_discovery_exposure_history()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    campaign_key_value text;
BEGIN
    SELECT campaign_key INTO STRICT campaign_key_value
    FROM systematic_fx.campaigns
    WHERE campaign_id = OLD.campaign_id;
    IF campaign_key_value = 'phase1a_conservative_screening_v1' THEN
        RAISE EXCEPTION 'Phase 1A Discovery exposures are append-preserved';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER discovery_exposures_preserve_phase1a_history
BEFORE UPDATE OR DELETE ON systematic_fx.discovery_exposures
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_phase1a_discovery_exposure_history();

COMMENT ON COLUMN systematic_fx.discovery_exposures.research_run_spec_id IS
    'Canonical all-variable RunSpec that produced this AI-visible exposure; mandatory '
    'and append-preserved for Phase 1A.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (7, 'governed_discovery_exposures', :'migration_checksum');

COMMIT;
