BEGIN;

-- Migration 0016 generalized the enforcement functions. Route the already
-- validated p5 rows through their original exact functions and p1_05 through
-- the ordered-candidate functions.  This also avoids dereferencing the p1-only
-- predecessor audit record on a p5 success transition.
DROP TRIGGER phase1a_outcome_manifests_preserve_and_validate
ON systematic_fx.phase1a_outcome_replay_manifests;

CREATE TRIGGER phase1a_p5_outcome_manifests_preserve_and_validate
BEFORE INSERT OR UPDATE
ON systematic_fx.phase1a_outcome_replay_manifests
FOR EACH ROW
WHEN (NEW.pattern_key = 'p5_01_range_expansion_flow_continuation')
EXECUTE FUNCTION systematic_fx.protect_phase1a_outcome_manifest();

CREATE TRIGGER phase1a_p1_05_outcome_manifests_preserve_and_validate
BEFORE INSERT OR UPDATE
ON systematic_fx.phase1a_outcome_replay_manifests
FOR EACH ROW
WHEN (NEW.pattern_key = 'p1_05_unconfirmed_move_reversal')
EXECUTE FUNCTION systematic_fx.protect_phase1a_ordered_outcome_manifest();

CREATE TRIGGER phase1a_outcome_manifests_append_preserved_delete
BEFORE DELETE
ON systematic_fx.phase1a_outcome_replay_manifests
FOR EACH ROW
EXECUTE FUNCTION systematic_fx.protect_phase1a_outcome_manifest();

DROP TRIGGER phase1a_outcome_manifest_completion_hardening
ON systematic_fx.phase1a_outcome_replay_manifests;

CREATE TRIGGER phase1a_p5_outcome_manifest_completion_hardening
BEFORE INSERT OR UPDATE
ON systematic_fx.phase1a_outcome_replay_manifests
FOR EACH ROW
WHEN (NEW.pattern_key = 'p5_01_range_expansion_flow_continuation')
EXECUTE FUNCTION systematic_fx.harden_phase1a_outcome_completion();

CREATE TRIGGER phase1a_p1_05_outcome_manifest_completion_hardening
BEFORE INSERT OR UPDATE
ON systematic_fx.phase1a_outcome_replay_manifests
FOR EACH ROW
WHEN (NEW.pattern_key = 'p1_05_unconfirmed_move_reversal')
EXECUTE FUNCTION systematic_fx.harden_phase1a_ordered_outcome_completion();

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (17, 'phase1a_ordered_trigger_routing', :'migration_checksum');

COMMIT;
