BEGIN;

ALTER TABLE systematic_fx.research_run_specs
    ADD COLUMN code_snapshot_sha256 text;

ALTER TABLE systematic_fx.research_run_specs
    ADD CONSTRAINT research_run_specs_code_snapshot_sha256_valid
        CHECK (code_snapshot_sha256 IS NULL
               OR code_snapshot_sha256 ~ '^[0-9a-f]{64}$');

CREATE FUNCTION systematic_fx.require_research_run_code_snapshot()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.code_snapshot_sha256 IS NULL THEN
        RAISE EXCEPTION 'new research run specifications require code_snapshot_sha256';
    END IF;
    IF NEW.canonical_spec ->> 'code_snapshot_sha256' IS DISTINCT FROM
       NEW.code_snapshot_sha256 THEN
        RAISE EXCEPTION 'canonical research spec and code snapshot identity differ';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER research_run_specs_require_code_snapshot
BEFORE INSERT ON systematic_fx.research_run_specs
FOR EACH ROW EXECUTE FUNCTION systematic_fx.require_research_run_code_snapshot();

COMMENT ON COLUMN systematic_fx.research_run_specs.code_snapshot_sha256 IS
    'Exact content snapshot of runtime code, configs, migrations, and research policy files; '
    'legacy pre-v2 rows may be null, but every new row is trigger-enforced.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (4, 'code_snapshot_provenance', :'migration_checksum');

COMMIT;
