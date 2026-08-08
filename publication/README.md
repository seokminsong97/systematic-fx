# Public research projection

The public site never connects to the private research database. Publication
is a one-way, retry-safe projection:

```text
private PostgreSQL
  -> transactional publication_outbox
  -> systematic-fx-publisher
  -> schema validation + canonical SHA-256
  -> isolated public PostgreSQL (append-only JSONB revisions)
  -> Next.js server-only reader
  -> /api/research/snapshot
  -> React live refresh
```

The worker reads the private control plane in a `REPEATABLE READ`, read-only
transaction so one public revision cannot mix states from different commits.
Publishing and acknowledgement span two databases, so the write is idempotent:
replaying the same campaign/revision is accepted only when its payload hash is
identical.

## Bootstrap and run

Apply the private migrations first. `provision-public` creates the isolated
database, a `NOLOGIN` owner, an insert-only publisher role, and a view-only site
reader. It rotates generated reader/writer passwords and writes them only to
ignored mode-`0600` runtime environment files:

```bash
systematic-fx-publisher --env-file /path/to/source/.env provision-public
systematic-fx-publisher once
systematic-fx-publisher watch --interval-seconds 5
```

Use `--runtime-env` and `--web-env` on `provision-public` when the publisher and
website environment files live outside the current checkout. The command does
not print credentials. `bootstrap-public` remains available for infrastructure
where an owner credential and roles are managed externally.

Required publisher settings are `SYSTEMATIC_FX_DATABASE_URL` for the private
research database and `SYSTEMATIC_FX_PUBLIC_DATABASE_URL` for the isolated
public database. They must not be the same database or credential.

The website gets a separate `SELECT`-only credential through
`web/.env.local` as `SITE_DATABASE_URL`. Grant that role schema usage and
`SELECT` on `systematic_fx_public.current_research_publications`; do not grant
it access to the private database or the append-only base table.

## Disclosure boundary

The versioned JSON Schema at
`contracts/publication/research-snapshot.v2.schema.json` is an allowlist. It
publishes governed RunSpec outcomes, Discovery rollups, ordered replay progress,
directional screening decisions, and equivalence-audit state while keeping each
status layer distinct. Raw market events, full outcome surfaces, selected
parameters, artifact URIs and hashes, filesystem paths, provider payloads,
canonical specs, credentials, and internal error text are intentionally absent.
Contract changes require a new schema version and a compatible website update.
