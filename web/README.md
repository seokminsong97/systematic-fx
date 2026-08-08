# Systematic FX Research Ledger

Public, read-only presentation layer for the Systematic FX research program.

## Architectural boundaries

```text
src/app/          Route composition, metadata, and request-time entrypoints only
src/features/     Research-domain UI grouped by user-facing capability
src/components/   Shared layout and presentation primitives
src/domain/       Public research types and status semantics
src/server/       Server-only database, repository, and query-service boundary
src/lib/          Framework-neutral validation, polling, and formatting helpers
src/styles/       Global tokens, reset, and shared visual utilities
tests/            Frontend unit and integration tests
```

The browser never connects to PostgreSQL. A publication worker projects the
complete allowlisted research document from the private research database into
a separate public projection database. Next.js Server Components query the
latest projection on every request, while Route Handlers expose the same JSON
document for live client refresh. Database modules may only be imported from
`src/server/`.

Route files stay deliberately thin. Domain decisions belong in `src/domain`,
data access belongs in `src/server/repositories`, and feature-specific
rendering belongs in `src/features`.

The application is deliberately dynamic: there is no `output: "export"`, no
build-time research JSON, and no ISR cache. A request reads the latest public
revision on the server; an open page then checks the Route Handler every 15
seconds by default. The worker's watch interval is independently configurable
and capped at 60 seconds.

## Publication boundary

The shared JSON response contract lives at
`../contracts/publication/research-snapshot.v2.schema.json`. The public
projection database stores that complete contract as versioned JSONB and
exposes it only to the application's `SELECT`-only role. Pages render on demand
and live components poll the sanitized Route Handler; no research-database row
or build-time snapshot is ever passed to a Client Component.

## Runtime settings

Copy `.env.example` to an ignored `.env.local` and set `SITE_DATABASE_URL` to a
PostgreSQL role with `SELECT` permission on
`systematic_fx_public.current_research_publications` only. The publisher uses a
different writer credential; the browser receives neither credential.
`systematic-fx-publisher provision-public` can create both roles and write this
ignored file without exposing either password in terminal output.

```bash
npm run dev
npm run typecheck
npm test
npm run build
```
