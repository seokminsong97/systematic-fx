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
adds freeze/lineage checks needed before experiments can become evidence.
`0003_research_run_ledger.sql` and `0004_code_snapshot_provenance.sql` add
immutable computation identity and exact code-snapshot provenance.
`0005_campaign_level_run_ownership.sql` closes the run-to-experiment ownership
boundary for performance-bearing computations. `0006_publication_outbox.sql`
adds the coalescing transactional outbox that signals the isolated public
projection worker. A prior migration remains immutable; all future changes
receive a new ordered file.

`0007_governed_discovery_exposures.sql` binds Phase 1A AI-visible exposures to
their canonical run specifications and preserves their history.

`0008` through `0012` harden Phase 1A rollups, artifact lineage, execution
atomicity, control artifacts, and campaign-level validation. `0013` through
`0019` register chronological outcome manifests, checkpoints, complete cell
surfaces, ordered candidates, directional screening decisions, and independent
equivalence audits with fail-closed lineage constraints. `0020` extends the
publication outbox to live RunSpec, attempt, and checkpoint progress so the
public projection refreshes while governed work advances.
