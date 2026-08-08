BEGIN;

CREATE TABLE systematic_fx.publication_outbox (
    scope_key text PRIMARY KEY,
    request_version bigint NOT NULL DEFAULT 1,
    delivered_version bigint NOT NULL DEFAULT 0,
    requested_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    claimed_at timestamptz,
    claimed_by text,
    attempts integer NOT NULL DEFAULT 0,
    last_error text,
    CONSTRAINT publication_outbox_scope_nonempty CHECK (btrim(scope_key) <> ''),
    CONSTRAINT publication_outbox_versions_valid
        CHECK (request_version > 0
               AND delivered_version >= 0
               AND delivered_version <= request_version),
    CONSTRAINT publication_outbox_attempts_nonnegative CHECK (attempts >= 0),
    CONSTRAINT publication_outbox_claim_pair
        CHECK ((claimed_at IS NULL) = (claimed_by IS NULL))
);

CREATE OR REPLACE FUNCTION systematic_fx.request_publication_refresh()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO systematic_fx.publication_outbox (
        scope_key, request_version, delivered_version, requested_at
    )
    VALUES ('public-research', 1, 0, statement_timestamp())
    ON CONFLICT (scope_key) DO UPDATE
    SET request_version = systematic_fx.publication_outbox.request_version + 1,
        requested_at = statement_timestamp(),
        last_error = NULL;
    RETURN NULL;
END;
$$;

CREATE TRIGGER datasets_publication_refresh
AFTER INSERT OR UPDATE OR DELETE ON systematic_fx.datasets
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();
CREATE TRIGGER source_files_publication_refresh
AFTER INSERT OR UPDATE OR DELETE ON systematic_fx.source_files
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();
CREATE TRIGGER quality_checks_publication_refresh
AFTER INSERT OR UPDATE OR DELETE ON systematic_fx.quality_checks
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();
CREATE TRIGGER campaigns_publication_refresh
AFTER INSERT OR UPDATE OR DELETE ON systematic_fx.campaigns
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();
CREATE TRIGGER campaign_splits_publication_refresh
AFTER INSERT OR UPDATE OR DELETE ON systematic_fx.campaign_splits
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();
CREATE TRIGGER campaign_days_publication_refresh
AFTER INSERT OR UPDATE OR DELETE ON systematic_fx.campaign_days
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();
CREATE TRIGGER pattern_ledger_publication_refresh
AFTER INSERT OR UPDATE OR DELETE ON systematic_fx.pattern_ledger
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();
CREATE TRIGGER experiments_publication_refresh
AFTER INSERT OR UPDATE OR DELETE ON systematic_fx.experiments
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();
CREATE TRIGGER experiment_trials_publication_refresh
AFTER INSERT OR UPDATE OR DELETE ON systematic_fx.experiment_trials
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();
CREATE TRIGGER strategies_publication_refresh
AFTER INSERT OR UPDATE OR DELETE ON systematic_fx.strategies
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();
CREATE TRIGGER backtest_runs_publication_refresh
AFTER INSERT OR UPDATE OR DELETE ON systematic_fx.backtest_runs
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();
CREATE TRIGGER backtest_metrics_publication_refresh
AFTER INSERT OR UPDATE OR DELETE ON systematic_fx.backtest_metrics
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();
CREATE TRIGGER discovery_exposures_publication_refresh
AFTER INSERT OR UPDATE OR DELETE ON systematic_fx.discovery_exposures
FOR EACH STATEMENT EXECUTE FUNCTION systematic_fx.request_publication_refresh();

INSERT INTO systematic_fx.publication_outbox (
    scope_key, request_version, delivered_version, requested_at
)
VALUES ('public-research', 1, 0, statement_timestamp())
ON CONFLICT (scope_key) DO UPDATE
SET request_version = systematic_fx.publication_outbox.request_version + 1,
    requested_at = statement_timestamp();

COMMENT ON TABLE systematic_fx.publication_outbox IS
    'Durable, coalescing signal for one-way publication into the isolated public projection database.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (6, 'publication_outbox', :'migration_checksum');

COMMIT;
