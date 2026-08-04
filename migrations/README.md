# PostgreSQL Migrations

Ordered SQL migrations own all PostgreSQL schema changes. PostgreSQL is the
research control plane; raw MBP-10 events and wide feature rows remain in
immutable Parquet.

Initialize the repository-private PostgreSQL 18 control plane, create the
database when absent, and apply migrations with:

```bash
make db-up
```

For an externally managed PostgreSQL target, configure both the admin and
application URLs in the ignored `.env`, then run `make db-bootstrap`.

The runner executes migrations in numeric filename order and records the exact
SHA-256 in `systematic_fx.schema_migrations`. A changed checksum for an applied
version is an error; create a new migration instead of editing history. Every
migration contains its own `BEGIN`/`COMMIT`, so a failed file leaves neither a
partial schema nor a migration record.

Integration tests require an explicitly disposable or repository-private
database. The test target is never inferred from the application URL:

```text
SYSTEMATIC_FX_TEST_DATABASE_URL=postgresql://...
```

Ad-hoc schema edits are not permitted.

`0002_research_governance.sql` records every AI-visible Discovery exposure and
adds freeze/lineage checks needed before experiments can become evidence. A
prior migration remains immutable; all future changes receive a new ordered
file.
