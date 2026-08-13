\set ON_ERROR_STOP on

BEGIN;

DO $roles$
DECLARE insecure_role text;
BEGIN
    SELECT rolname INTO insecure_role FROM pg_roles
     WHERE rolname IN ('systematic_fx_holdout_owner',
                       'systematic_fx_research_daemon',
                       'systematic_fx_holdout_executor',
                       'systematic_fx_m0b_worker_api_owner',
                       'systematic_fx_m0b_worker')
       AND (rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole OR rolinherit
            OR rolreplication OR rolbypassrls)
     ORDER BY rolname LIMIT 1;
    IF insecure_role IS NOT NULL THEN
        RAISE EXCEPTION 'existing role % has unsafe attributes; provisioning refuses mutation',
            insecure_role;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'systematic_fx_holdout_owner') THEN
        CREATE ROLE systematic_fx_holdout_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'systematic_fx_research_daemon') THEN
        CREATE ROLE systematic_fx_research_daemon NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'systematic_fx_holdout_executor') THEN
        CREATE ROLE systematic_fx_holdout_executor NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles
                    WHERE rolname = 'systematic_fx_m0b_worker_api_owner') THEN
        CREATE ROLE systematic_fx_m0b_worker_api_owner NOLOGIN NOSUPERUSER NOCREATEDB
            NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'systematic_fx_m0b_worker') THEN
        CREATE ROLE systematic_fx_m0b_worker NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
END
$roles$;

DO $worker_database_boundary$
BEGIN
    EXECUTE format(
        'REVOKE CREATE ON DATABASE %I FROM systematic_fx_m0b_worker',
        current_database());
END
$worker_database_boundary$;

DO $worker_api$
DECLARE capability record;
BEGIN
    IF to_regclass('systematic_fx.m0b_worker_leases') IS NULL
       OR to_regclass('systematic_fx.m0b_admission_decisions') IS NULL THEN
        RAISE EXCEPTION 'M0b worker capability migration 0030 is not installed';
    END IF;
    FOR capability IN
        SELECT * FROM (VALUES
            ('m0b_worker_claim_next(text,text,text,integer)'),
            ('m0b_worker_checkpoint(bigint,bigint,text,integer,text,text,jsonb)'),
            ('m0b_worker_terminalize(bigint,bigint,text,text,bigint,jsonb)'),
            ('m0b_worker_fail(bigint,bigint,text,text,boolean)')
        ) AS item(signature)
    LOOP
        IF to_regprocedure('systematic_fx.' || capability.signature) IS NULL THEN
            RAISE EXCEPTION 'required M0b worker capability is absent: %',
                capability.signature;
        END IF;
        EXECUTE format(
            'ALTER FUNCTION systematic_fx.%s OWNER TO systematic_fx_m0b_worker_api_owner',
            capability.signature);
        EXECUTE format('REVOKE ALL ON FUNCTION systematic_fx.%s FROM PUBLIC',
                       capability.signature);
        EXECUTE format(
            'REVOKE ALL ON FUNCTION systematic_fx.%s FROM systematic_fx_research_daemon, systematic_fx_holdout_executor',
            capability.signature);
        EXECUTE format(
            'GRANT EXECUTE ON FUNCTION systematic_fx.%s TO systematic_fx_m0b_worker',
            capability.signature);
    END LOOP;
END
$worker_api$;

GRANT USAGE ON SCHEMA systematic_fx TO systematic_fx_m0b_worker;
GRANT USAGE ON SCHEMA systematic_fx TO systematic_fx_m0b_worker_api_owner;
GRANT EXECUTE ON FUNCTION systematic_fx.m0b_worker_authorized(),
    systematic_fx.m0b_numeric_admission_rules_valid(jsonb),
    systematic_fx.m0b_numeric_metrics_valid(jsonb),
    systematic_fx.m0b_numeric_metrics_admitted(jsonb,jsonb),
    systematic_fx.m0b_worker_checkpoint_state_valid(jsonb,jsonb),
    systematic_fx.canonical_jsonb_sha256(jsonb)
    TO systematic_fx_m0b_worker_api_owner;
GRANT SELECT ON systematic_fx.campaigns, systematic_fx.m0b_epochs,
    systematic_fx.m0b_candidates, systematic_fx.research_run_attempts,
    systematic_fx.m0b_worker_leases, systematic_fx.m0b_admission_decisions,
    systematic_fx.artifacts, systematic_fx.m0b_artifact_links,
    systematic_fx.m0b_checkpoints TO systematic_fx_m0b_worker_api_owner;
GRANT SELECT ON systematic_fx.research_run_specs, systematic_fx.experiments,
    systematic_fx.experiment_trials, systematic_fx.phase1a_outcome_replay_manifests,
    systematic_fx.bar_state_artifact_links, systematic_fx.publication_outbox
    TO systematic_fx_m0b_worker_api_owner;
-- PostgreSQL requires UPDATE privilege for the row-locking clauses used by
-- the governance triggers, even though these dependency rows are never
-- changed by the four worker API bodies.
GRANT UPDATE ON systematic_fx.campaigns, systematic_fx.m0b_epochs,
    systematic_fx.research_run_specs, systematic_fx.experiments
    TO systematic_fx_m0b_worker_api_owner;
GRANT INSERT, UPDATE ON systematic_fx.research_run_attempts,
    systematic_fx.m0b_candidates, systematic_fx.m0b_worker_leases
    TO systematic_fx_m0b_worker_api_owner;
GRANT INSERT ON systematic_fx.m0b_checkpoints,
    systematic_fx.m0b_admission_decisions, systematic_fx.artifacts,
    systematic_fx.m0b_artifact_links TO systematic_fx_m0b_worker_api_owner;
GRANT INSERT, UPDATE ON systematic_fx.publication_outbox
    TO systematic_fx_m0b_worker_api_owner;
GRANT USAGE, SELECT ON SEQUENCE
    systematic_fx.research_run_attempts_research_run_attempt_id_seq,
    systematic_fx.m0b_checkpoints_m0b_checkpoint_id_seq,
    systematic_fx.m0b_admission_decisions_m0b_admission_decision_id_seq,
    systematic_fx.artifacts_artifact_id_seq,
    systematic_fx.m0b_artifact_links_m0b_artifact_link_id_seq
    TO systematic_fx_m0b_worker_api_owner;

REVOKE ALL ON ALL TABLES IN SCHEMA systematic_fx FROM systematic_fx_m0b_worker;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA systematic_fx FROM systematic_fx_m0b_worker;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA systematic_fx FROM systematic_fx_m0b_worker;
REVOKE CREATE ON SCHEMA systematic_fx FROM systematic_fx_m0b_worker;
GRANT SELECT ON systematic_fx.campaigns, systematic_fx.m0b_epochs,
    systematic_fx.m0b_candidates, systematic_fx.research_run_attempts,
    systematic_fx.m0b_admission_decisions, systematic_fx.artifacts,
    systematic_fx.m0b_artifact_links, systematic_fx.m0b_checkpoints
    TO systematic_fx_m0b_worker;

-- The blanket function revoke above is intentional; restore only the exact
-- four mutation capabilities after every idempotent provisioning pass.
GRANT EXECUTE ON FUNCTION systematic_fx.m0b_worker_claim_next(text,text,text,integer)
    TO systematic_fx_m0b_worker;
GRANT EXECUTE ON FUNCTION systematic_fx.m0b_worker_checkpoint(
    bigint,bigint,text,integer,text,text,jsonb) TO systematic_fx_m0b_worker;
GRANT EXECUTE ON FUNCTION systematic_fx.m0b_worker_terminalize(
    bigint,bigint,text,text,bigint,jsonb) TO systematic_fx_m0b_worker;
GRANT EXECUTE ON FUNCTION systematic_fx.m0b_worker_fail(
    bigint,bigint,text,text,boolean) TO systematic_fx_m0b_worker;

DO $sealed_objects$
DECLARE
    actual_owner text;
    actual_columns jsonb;
    actual_constraints jsonb;
    expected_columns constant jsonb := '[
      {"name":"holdout_artifact_id","type":"bigint","not_null":true,"identity":"a"},
      {"name":"artifact_key","type":"text","not_null":true,"identity":""},
      {"name":"storage_locator","type":"text","not_null":true,"identity":""},
      {"name":"sha256","type":"text","not_null":true,"identity":""},
      {"name":"byte_size","type":"bigint","not_null":true,"identity":""},
      {"name":"sealed_at","type":"timestamp with time zone","not_null":true,"identity":""},
      {"name":"authorization_sha256","type":"text","not_null":false,"identity":""}
    ]'::jsonb;
BEGIN
    SELECT role.rolname INTO actual_owner
      FROM pg_namespace AS namespace
      JOIN pg_roles AS role ON role.oid = namespace.nspowner
     WHERE namespace.nspname = 'systematic_fx_sealed';
    IF FOUND AND actual_owner <> 'systematic_fx_holdout_owner' THEN
        RAISE EXCEPTION 'existing sealed schema has unexpected owner %', actual_owner;
    ELSIF NOT FOUND THEN
        CREATE SCHEMA systematic_fx_sealed AUTHORIZATION systematic_fx_holdout_owner;
    END IF;

    IF to_regclass('systematic_fx_sealed.holdout_artifacts') IS NULL THEN
        CREATE TABLE systematic_fx_sealed.holdout_artifacts (
            holdout_artifact_id bigint GENERATED ALWAYS AS IDENTITY,
            artifact_key text NOT NULL,
            storage_locator text NOT NULL,
            sha256 text NOT NULL,
            byte_size bigint NOT NULL,
            sealed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
            authorization_sha256 text,
            CONSTRAINT holdout_artifacts_pk PRIMARY KEY (holdout_artifact_id),
            CONSTRAINT holdout_artifacts_artifact_key_unique UNIQUE (artifact_key),
            CONSTRAINT holdout_artifacts_sha256_valid
                CHECK (sha256 ~ '^[0-9a-f]{64}$'),
            CONSTRAINT holdout_artifacts_byte_size_nonnegative CHECK (byte_size >= 0),
            CONSTRAINT holdout_artifacts_authorization_sha256_valid CHECK (
                authorization_sha256 IS NULL OR authorization_sha256 ~ '^[0-9a-f]{64}$')
        );
        ALTER TABLE systematic_fx_sealed.holdout_artifacts
            OWNER TO systematic_fx_holdout_owner;
    END IF;

    SELECT role.rolname INTO actual_owner
      FROM pg_class AS relation
      JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
      JOIN pg_roles AS role ON role.oid = relation.relowner
     WHERE namespace.nspname = 'systematic_fx_sealed'
       AND relation.relname = 'holdout_artifacts'
       AND relation.relkind = 'r' AND relation.relpersistence = 'p';
    IF NOT FOUND OR actual_owner <> 'systematic_fx_holdout_owner' THEN
        RAISE EXCEPTION 'sealed holdout table has unexpected type or owner %', actual_owner;
    END IF;
    SELECT jsonb_agg(jsonb_build_object(
               'name', attribute.attname,
               'type', pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
               'not_null', attribute.attnotnull,
               'identity', attribute.attidentity::text)
               ORDER BY attribute.attnum)
      INTO actual_columns
      FROM pg_attribute AS attribute
      JOIN pg_class AS relation ON relation.oid = attribute.attrelid
      JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
     WHERE namespace.nspname = 'systematic_fx_sealed'
       AND relation.relname = 'holdout_artifacts'
       AND attribute.attnum > 0 AND NOT attribute.attisdropped;
    IF actual_columns IS DISTINCT FROM expected_columns THEN
        RAISE EXCEPTION 'sealed holdout table has unexpected column identity: %', actual_columns;
    END IF;
    SELECT jsonb_object_agg(constraint_row.conname, constraint_row.definition)
      INTO actual_constraints
      FROM (
        SELECT constraint_row.conname,
               pg_catalog.pg_get_constraintdef(constraint_row.oid, true) AS definition
          FROM pg_constraint AS constraint_row
         WHERE constraint_row.conrelid =
               'systematic_fx_sealed.holdout_artifacts'::regclass
         ORDER BY constraint_row.conname
      ) AS constraint_row;
    IF actual_constraints IS DISTINCT FROM jsonb_build_object(
        'holdout_artifacts_pk', 'PRIMARY KEY (holdout_artifact_id)',
        'holdout_artifacts_holdout_artifact_id_not_null',
            'NOT NULL holdout_artifact_id',
        'holdout_artifacts_artifact_key_unique', 'UNIQUE (artifact_key)',
        'holdout_artifacts_artifact_key_not_null', 'NOT NULL artifact_key',
        'holdout_artifacts_storage_locator_not_null', 'NOT NULL storage_locator',
        'holdout_artifacts_sha256_valid',
            'CHECK (sha256 ~ ''^[0-9a-f]{64}$''::text)',
        'holdout_artifacts_sha256_not_null', 'NOT NULL sha256',
        'holdout_artifacts_byte_size_nonnegative', 'CHECK (byte_size >= 0)',
        'holdout_artifacts_byte_size_not_null', 'NOT NULL byte_size',
        'holdout_artifacts_sealed_at_not_null', 'NOT NULL sealed_at',
        'holdout_artifacts_authorization_sha256_valid',
            'CHECK (authorization_sha256 IS NULL OR authorization_sha256 ~ ''^[0-9a-f]{64}$''::text)'
    ) THEN
        RAISE EXCEPTION 'sealed holdout table has unexpected constraints: %',
            actual_constraints;
    END IF;
    IF pg_catalog.pg_get_expr(
           (SELECT adbin FROM pg_attrdef
             WHERE adrelid = 'systematic_fx_sealed.holdout_artifacts'::regclass
               AND adnum = 6),
           'systematic_fx_sealed.holdout_artifacts'::regclass
       ) IS DISTINCT FROM 'statement_timestamp()' THEN
        RAISE EXCEPTION 'sealed_at default identity is invalid';
    END IF;
END
$sealed_objects$;

REVOKE ALL ON SCHEMA systematic_fx_sealed FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA systematic_fx_sealed FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA systematic_fx_sealed FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA systematic_fx_sealed FROM PUBLIC;

REVOKE ALL ON SCHEMA systematic_fx_sealed FROM systematic_fx_research_daemon;
REVOKE CREATE ON SCHEMA systematic_fx FROM systematic_fx_research_daemon;
REVOKE ALL ON ALL TABLES IN SCHEMA systematic_fx_sealed
    FROM systematic_fx_research_daemon;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA systematic_fx_sealed
    FROM systematic_fx_research_daemon;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA systematic_fx_sealed
    FROM systematic_fx_research_daemon;
REVOKE ALL ON SCHEMA systematic_fx_sealed FROM systematic_fx_m0b_worker;
REVOKE ALL ON ALL TABLES IN SCHEMA systematic_fx_sealed
    FROM systematic_fx_m0b_worker;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA systematic_fx_sealed
    FROM systematic_fx_m0b_worker;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA systematic_fx_sealed
    FROM systematic_fx_m0b_worker;
REVOKE ALL ON systematic_fx.datasets, systematic_fx.source_files,
    systematic_fx.instruments, systematic_fx.campaigns,
    systematic_fx.campaign_splits, systematic_fx.campaign_days,
    systematic_fx.pattern_ledger, systematic_fx.experiment_trials,
    systematic_fx.strategies, systematic_fx.backtest_runs,
    systematic_fx.experiments, systematic_fx.m0b_epochs,
    systematic_fx.artifacts, systematic_fx.research_run_specs,
    systematic_fx.research_run_attempts, systematic_fx.m0b_candidates,
    systematic_fx.m0b_checkpoints, systematic_fx.m0b_artifact_links,
    systematic_fx.m0b_admission_decisions, systematic_fx.m0b_worker_leases
    FROM systematic_fx_research_daemon;
REVOKE ALL ON SCHEMA systematic_fx_sealed FROM systematic_fx_holdout_executor;
REVOKE ALL ON ALL TABLES IN SCHEMA systematic_fx_sealed
    FROM systematic_fx_holdout_executor;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA systematic_fx_sealed
    FROM systematic_fx_holdout_executor;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA systematic_fx_sealed
    FROM systematic_fx_holdout_executor;
GRANT USAGE ON SCHEMA systematic_fx_sealed TO systematic_fx_holdout_executor;
GRANT SELECT ON systematic_fx_sealed.holdout_artifacts
    TO systematic_fx_holdout_executor;

ALTER DEFAULT PRIVILEGES FOR ROLE systematic_fx_holdout_owner
    IN SCHEMA systematic_fx_sealed
    REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE systematic_fx_holdout_owner
    IN SCHEMA systematic_fx_sealed
    REVOKE ALL ON TABLES FROM systematic_fx_research_daemon;
ALTER DEFAULT PRIVILEGES FOR ROLE systematic_fx_holdout_owner
    IN SCHEMA systematic_fx_sealed
    GRANT SELECT ON TABLES TO systematic_fx_holdout_executor;
ALTER DEFAULT PRIVILEGES FOR ROLE systematic_fx_holdout_owner
    IN SCHEMA systematic_fx_sealed
    REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE systematic_fx_holdout_owner
    IN SCHEMA systematic_fx_sealed
    REVOKE ALL ON SEQUENCES FROM systematic_fx_research_daemon;
ALTER DEFAULT PRIVILEGES FOR ROLE systematic_fx_holdout_owner
    IN SCHEMA systematic_fx_sealed
    REVOKE ALL ON SEQUENCES FROM systematic_fx_holdout_executor;
ALTER DEFAULT PRIVILEGES FOR ROLE systematic_fx_holdout_owner
    IN SCHEMA systematic_fx_sealed
    REVOKE ALL ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE systematic_fx_holdout_owner
    IN SCHEMA systematic_fx_sealed
    REVOKE ALL ON FUNCTIONS FROM systematic_fx_research_daemon;
ALTER DEFAULT PRIVILEGES FOR ROLE systematic_fx_holdout_owner
    IN SCHEMA systematic_fx_sealed
    REVOKE ALL ON FUNCTIONS FROM systematic_fx_holdout_executor;

DO $membership$
BEGIN
    IF pg_has_role('systematic_fx_research_daemon',
                   'systematic_fx_holdout_executor', 'MEMBER')
       OR pg_has_role('systematic_fx_holdout_executor',
                      'systematic_fx_research_daemon', 'MEMBER')
       OR pg_has_role('systematic_fx_research_daemon',
                      'systematic_fx_holdout_owner', 'MEMBER')
       OR pg_has_role('systematic_fx_holdout_executor',
                      'systematic_fx_holdout_owner', 'MEMBER')
       OR pg_has_role('systematic_fx_m0b_worker',
                      'systematic_fx_research_daemon', 'MEMBER')
       OR pg_has_role('systematic_fx_research_daemon',
                      'systematic_fx_m0b_worker', 'MEMBER')
       OR pg_has_role('systematic_fx_m0b_worker',
                      'systematic_fx_holdout_executor', 'MEMBER')
       OR pg_has_role('systematic_fx_m0b_worker',
                      'systematic_fx_holdout_owner', 'MEMBER')
       OR pg_has_role('systematic_fx_m0b_worker',
                      'systematic_fx_m0b_worker_api_owner', 'MEMBER')
       OR EXISTS (
            SELECT 1 FROM pg_roles AS target
             WHERE target.rolname <> 'systematic_fx_m0b_worker'
               AND pg_has_role('systematic_fx_m0b_worker', target.rolname, 'MEMBER')
       )
       OR EXISTS (
            SELECT 1 FROM pg_roles AS target
             WHERE target.rolname <> 'systematic_fx_m0b_worker_api_owner'
               AND pg_has_role('systematic_fx_m0b_worker_api_owner',
                               target.rolname, 'MEMBER')
       )
       OR EXISTS (
            SELECT 1 FROM pg_roles AS target
             WHERE target.rolname NOT IN ('systematic_fx_research_daemon',
                                          'systematic_fx_holdout_executor')
               AND (pg_has_role('systematic_fx_research_daemon', target.rolname, 'MEMBER')
                    OR pg_has_role('systematic_fx_holdout_executor', target.rolname, 'MEMBER'))
       ) THEN
        RAISE EXCEPTION 'research/holdout roles have unsafe transitive membership';
    END IF;
END
$membership$;

DO $capabilities$
DECLARE unsafe_capability text;
BEGIN
    SELECT format('%I.%I', namespace.nspname, routine.proname)
      INTO unsafe_capability
      FROM pg_proc AS routine
      JOIN pg_namespace AS namespace ON namespace.oid = routine.pronamespace
     WHERE routine.prosecdef
       AND namespace.nspname NOT IN ('pg_catalog', 'information_schema')
       AND has_function_privilege(
           'systematic_fx_research_daemon', routine.oid, 'EXECUTE')
     ORDER BY namespace.nspname, routine.proname
     LIMIT 1;
    IF unsafe_capability IS NOT NULL THEN
        RAISE EXCEPTION 'research role can execute SECURITY DEFINER capability %',
            unsafe_capability;
    END IF;
    SELECT format('%I.%I', namespace.nspname, relation.relname)
      INTO unsafe_capability
      FROM pg_class AS relation
      JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
      JOIN pg_roles AS owner ON owner.oid = relation.relowner
     WHERE owner.rolname = 'systematic_fx_holdout_owner'
       AND namespace.nspname <> 'systematic_fx_sealed'
       AND namespace.nspname NOT IN ('pg_catalog', 'information_schema')
       AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
     ORDER BY namespace.nspname, relation.relname
     LIMIT 1;
    IF unsafe_capability IS NOT NULL THEN
        RAISE EXCEPTION 'holdout owner controls relation outside sealed schema: %',
            unsafe_capability;
    END IF;
END
$capabilities$;

COMMIT;

-- These are NOLOGIN group roles. Deployment must grant the research role only
-- to the daemon login and the executor role only to a separately provisioned,
-- non-interactive credential. This script creates no password or login token.
