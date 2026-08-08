BEGIN;

ALTER TABLE systematic_fx.phase1a_outcome_cell_summaries
    VALIDATE CONSTRAINT phase1a_outcome_cells_frozen_signal_count,
    VALIDATE CONSTRAINT phase1a_outcome_cells_frozen_cost_accounting;

COMMENT ON CONSTRAINT phase1a_outcome_cells_frozen_signal_count
ON systematic_fx.phase1a_outcome_cell_summaries IS
    'Validated frozen p5 evidence boundary: LONG has 529 signals and SHORT has 582.';
COMMENT ON CONSTRAINT phase1a_outcome_cells_frozen_cost_accounting
ON systematic_fx.phase1a_outcome_cell_summaries IS
    'Validated per-fill costs: BASELINE 4+4, MODERATE 5+5, and SEVERE 6+6 ticks.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (15, 'phase1a_outcome_constraints_validated', :'migration_checksum');

COMMIT;
