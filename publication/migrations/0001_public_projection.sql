CREATE SCHEMA IF NOT EXISTS systematic_fx_public;

CREATE TABLE IF NOT EXISTS systematic_fx_public.schema_migrations (
    version integer PRIMARY KEY,
    name text NOT NULL UNIQUE,
    checksum text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT schema_migrations_version_positive CHECK (version > 0),
    CONSTRAINT schema_migrations_checksum_sha256 CHECK (checksum ~ '^[0-9a-f]{64}$')
);

CREATE TABLE systematic_fx_public.research_publications (
    publication_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    campaign_key text NOT NULL,
    revision bigint NOT NULL,
    schema_version text NOT NULL,
    payload jsonb NOT NULL,
    payload_sha256 text NOT NULL,
    source_commit text NOT NULL,
    generated_at timestamptz NOT NULL,
    published_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT research_publications_identity UNIQUE (campaign_key, revision),
    CONSTRAINT research_publications_campaign_nonempty CHECK (btrim(campaign_key) <> ''),
    CONSTRAINT research_publications_revision_positive CHECK (revision > 0),
    CONSTRAINT research_publications_schema_nonempty CHECK (btrim(schema_version) <> ''),
    CONSTRAINT research_publications_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT research_publications_sha256 CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT research_publications_commit_nonempty CHECK (btrim(source_commit) <> '')
);

CREATE INDEX research_publications_latest_idx
    ON systematic_fx_public.research_publications (campaign_key, revision DESC);

CREATE OR REPLACE VIEW systematic_fx_public.current_research_publications AS
SELECT DISTINCT ON (campaign_key)
       campaign_key,
       revision,
       schema_version,
       payload,
       payload_sha256,
       source_commit,
       generated_at,
       published_at
FROM systematic_fx_public.research_publications
ORDER BY campaign_key, revision DESC;

COMMENT ON TABLE systematic_fx_public.research_publications IS
    'Append-only, allowlisted JSON documents projected from the private research database.';
COMMENT ON VIEW systematic_fx_public.current_research_publications IS
    'Latest public document per campaign; grant the website reader SELECT on this view only.';
