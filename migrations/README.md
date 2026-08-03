# PostgreSQL Migrations

Alembic migrations will own all PostgreSQL schema changes. The database stores
catalogs and research state—not 154GB of raw market events.

The first schema migration should create tables for source files, instrument
mappings, data-quality checks, campaigns and folds, experiments, strategies,
backtest runs, metrics, artifacts, and job state. Ad-hoc production schema edits
are not permitted.
