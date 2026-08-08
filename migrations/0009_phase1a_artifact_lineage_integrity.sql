BEGIN;

CREATE FUNCTION systematic_fx.phase1a_artifact_is_protected(
    target_artifact_id bigint
)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
    SELECT
        EXISTS (
            SELECT 1
            FROM systematic_fx.artifacts AS artifact
            WHERE artifact.artifact_id = target_artifact_id
              AND artifact.artifact_type = 'PHASE1A_FEATURE_BUILD_MANIFEST'
        )
        OR EXISTS (
            SELECT 1
            FROM systematic_fx.discovery_exposures AS exposure
            JOIN systematic_fx.campaigns AS campaign
              ON campaign.campaign_id = exposure.campaign_id
            WHERE exposure.result_artifact_id = target_artifact_id
              AND campaign.campaign_key = 'phase1a_conservative_screening_v1'
        )
        OR EXISTS (
            SELECT 1
            FROM systematic_fx.research_run_attempts AS attempt
            JOIN systematic_fx.research_run_specs AS run_spec
              ON run_spec.research_run_spec_id = attempt.research_run_spec_id
            JOIN systematic_fx.campaigns AS campaign
              ON campaign.campaign_id = run_spec.campaign_id
            WHERE (attempt.result_artifact_id = target_artifact_id
                   OR attempt.trade_ledger_artifact_id = target_artifact_id)
              AND campaign.campaign_key = 'phase1a_conservative_screening_v1'
        )
        OR EXISTS (
            SELECT 1
            FROM systematic_fx.pattern_ledger AS pattern
            JOIN systematic_fx.campaigns AS campaign
              ON campaign.campaign_id = pattern.campaign_id
            WHERE pattern.context_artifact_id = target_artifact_id
              AND campaign.campaign_key = 'phase1a_conservative_screening_v1'
        );
$$;

CREATE FUNCTION systematic_fx.reject_phase1a_artifact_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    -- OLD is deliberately the only transition record inspected: NEW does not
    -- exist for DELETE, and an UPDATE must not escape protection by rewriting
    -- its type or a content/lineage field in the same statement.
    IF systematic_fx.phase1a_artifact_is_protected(OLD.artifact_id) THEN
        RAISE EXCEPTION 'Phase 1A result and lineage artifacts are immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER artifacts_protect_phase1a_lineage
BEFORE UPDATE OR DELETE ON systematic_fx.artifacts
FOR EACH ROW EXECUTE FUNCTION systematic_fx.reject_phase1a_artifact_mutation();

CREATE FUNCTION systematic_fx.protect_phase1a_campaign_identity()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF OLD.campaign_key <> 'phase1a_conservative_screening_v1' THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Phase 1A campaign identity cannot be deleted';
    END IF;
    IF NEW.campaign_key IS DISTINCT FROM OLD.campaign_key
       OR NEW.dataset_id IS DISTINCT FROM OLD.dataset_id THEN
        RAISE EXCEPTION 'Phase 1A campaign identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER campaigns_protect_phase1a_identity
BEFORE UPDATE OR DELETE ON systematic_fx.campaigns
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_phase1a_campaign_identity();

CREATE FUNCTION systematic_fx.protect_phase1a_attempt_artifact_links()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    phase1a boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM systematic_fx.research_run_specs AS run_spec
        JOIN systematic_fx.campaigns AS campaign
          ON campaign.campaign_id = run_spec.campaign_id
        WHERE run_spec.research_run_spec_id = OLD.research_run_spec_id
          AND campaign.campaign_key = 'phase1a_conservative_screening_v1'
    ) INTO phase1a;

    IF phase1a
       AND ((OLD.result_artifact_id IS NOT NULL
             AND NEW.result_artifact_id IS DISTINCT FROM OLD.result_artifact_id)
            OR (OLD.trade_ledger_artifact_id IS NOT NULL
                AND NEW.trade_ledger_artifact_id
                    IS DISTINCT FROM OLD.trade_ledger_artifact_id)) THEN
        RAISE EXCEPTION 'Phase 1A run-attempt artifact links are immutable once assigned';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER research_run_attempts_protect_phase1a_artifact_links
BEFORE UPDATE ON systematic_fx.research_run_attempts
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_phase1a_attempt_artifact_links();

CREATE FUNCTION systematic_fx.phase1a_feature_partition_is_protected(
    target_partition_id bigint
)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM systematic_fx.derived_partitions AS partition
        LEFT JOIN systematic_fx.artifacts AS manifest
          ON manifest.artifact_id = partition.manifest_artifact_id
        WHERE partition.derived_partition_id = target_partition_id
          AND (
              manifest.artifact_type = 'PHASE1A_FEATURE_BUILD_MANIFEST'
              OR (
                  partition.partition_key LIKE 'phase1a-feature:v1:%'
                  AND partition.definition_version = 'phase1a_mbp10_screening_v1'
                  AND EXISTS (
                      SELECT 1
                      FROM systematic_fx.research_run_specs AS run_spec
                      JOIN systematic_fx.campaigns AS campaign
                        ON campaign.campaign_id = run_spec.campaign_id
                      WHERE run_spec.research_run_spec_id = CASE
                          WHEN partition.metadata #>>
                                   '{provenance,research_run_spec_id}' ~ '^[0-9]+$'
                          THEN (partition.metadata #>>
                                    '{provenance,research_run_spec_id}')::bigint
                          ELSE NULL
                      END
                        AND campaign.campaign_key =
                            'phase1a_conservative_screening_v1'
                  )
              )
          )
    );
$$;

CREATE FUNCTION systematic_fx.reject_phase1a_feature_partition_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF systematic_fx.phase1a_feature_partition_is_protected(
        OLD.derived_partition_id
    ) THEN
        RAISE EXCEPTION 'Phase 1A feature partitions are immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER derived_partitions_protect_phase1a_lineage
BEFORE UPDATE OR DELETE ON systematic_fx.derived_partitions
FOR EACH ROW EXECUTE FUNCTION systematic_fx.reject_phase1a_feature_partition_mutation();

CREATE FUNCTION systematic_fx.protect_phase1a_derived_partition_source()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    old_is_phase1a boolean;
    new_is_phase1a boolean;
    source_matches_manifest boolean;
BEGIN
    IF TG_OP = 'DELETE' THEN
        old_is_phase1a := systematic_fx.phase1a_feature_partition_is_protected(
            OLD.derived_partition_id
        );
        IF old_is_phase1a THEN
            RAISE EXCEPTION 'Phase 1A feature source links are immutable';
        END IF;
        RETURN OLD;
    END IF;

    new_is_phase1a := systematic_fx.phase1a_feature_partition_is_protected(
        NEW.derived_partition_id
    );
    IF TG_OP = 'UPDATE' THEN
        old_is_phase1a := systematic_fx.phase1a_feature_partition_is_protected(
            OLD.derived_partition_id
        );
        IF old_is_phase1a OR new_is_phase1a THEN
            RAISE EXCEPTION 'Phase 1A feature source links are immutable';
        END IF;
        RETURN NEW;
    END IF;

    IF NOT new_is_phase1a THEN
        RETURN NEW;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM systematic_fx.derived_partitions AS partition
        JOIN systematic_fx.source_files AS source
          ON source.source_file_id = NEW.source_file_id
         AND source.dataset_id = partition.dataset_id
        WHERE partition.derived_partition_id = NEW.derived_partition_id
          AND source.sha256 = NEW.source_sha256
          AND (
              (
                  partition.metadata #>>
                      '{provenance,current_source,relative_uri}' = source.relative_uri
                  AND partition.metadata #>>
                      '{provenance,current_source,source_date}' = source.source_date::text
                  AND partition.metadata #>>
                      '{provenance,current_source,sha256}' = NEW.source_sha256
              )
              OR (
                  partition.metadata #>>
                      '{provenance,previous_source,relative_uri}' = source.relative_uri
                  AND partition.metadata #>>
                      '{provenance,previous_source,source_date}' = source.source_date::text
                  AND partition.metadata #>>
                      '{provenance,previous_source,sha256}' = NEW.source_sha256
              )
          )
    ) INTO source_matches_manifest;

    IF NOT source_matches_manifest THEN
        RAISE EXCEPTION 'Phase 1A feature source link differs from partition provenance';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER derived_partition_sources_protect_phase1a_lineage
BEFORE INSERT OR UPDATE OR DELETE ON systematic_fx.derived_partition_sources
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_phase1a_derived_partition_source();

CREATE FUNCTION systematic_fx.protect_phase1a_source_file_identity()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    used_by_phase1a boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM systematic_fx.derived_partition_sources AS link
        WHERE link.source_file_id = OLD.source_file_id
          AND systematic_fx.phase1a_feature_partition_is_protected(
              link.derived_partition_id
          )
    ) INTO used_by_phase1a;

    IF NOT used_by_phase1a THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Phase 1A feature source files cannot be deleted';
    END IF;
    IF NEW.dataset_id IS DISTINCT FROM OLD.dataset_id
       OR NEW.source_date IS DISTINCT FROM OLD.source_date
       OR NEW.relative_uri IS DISTINCT FROM OLD.relative_uri
       OR NEW.byte_size IS DISTINCT FROM OLD.byte_size
       OR NEW.sha256 IS DISTINCT FROM OLD.sha256
       OR NEW.row_count IS DISTINCT FROM OLD.row_count
       OR NEW.parquet_schema_fingerprint IS DISTINCT FROM OLD.parquet_schema_fingerprint
       OR NEW.min_event_time_ns IS DISTINCT FROM OLD.min_event_time_ns
       OR NEW.max_event_time_ns IS DISTINCT FROM OLD.max_event_time_ns THEN
        RAISE EXCEPTION 'Phase 1A feature source-file identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER source_files_protect_phase1a_lineage
BEFORE UPDATE OR DELETE ON systematic_fx.source_files
FOR EACH ROW EXECUTE FUNCTION systematic_fx.protect_phase1a_source_file_identity();

COMMENT ON FUNCTION systematic_fx.phase1a_artifact_is_protected(bigint) IS
    'True for artifacts used by Phase 1A Discovery, run attempts, pattern context, '
    'or the governed feature-build manifest.';
COMMENT ON TRIGGER artifacts_protect_phase1a_lineage ON systematic_fx.artifacts IS
    'Freezes all identity, content, media, metadata, and producer-lineage fields of '
    'Phase 1A result and feature-manifest artifacts.';
COMMENT ON TRIGGER derived_partitions_protect_phase1a_lineage
    ON systematic_fx.derived_partitions IS
    'Freezes governed Phase 1A feature partition identity, content, and provenance.';

INSERT INTO systematic_fx.schema_migrations (version, name, checksum)
VALUES (9, 'phase1a_artifact_lineage_integrity', :'migration_checksum');

COMMIT;
