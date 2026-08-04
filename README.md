# systematic-fx
Systematic FX research and execution platform — tick-level data pipeline, deterministic backtesting, and risk-gated execution (EUR/USD)

Data source:
[`mbo-mbp10-converter`](https://github.com/seokminsong97/mbo-mbp10-converter)

## Research environment

The reproducible environment uses `uv.lock`, all project extras, and Python
3.12.13. PostgreSQL 18.4 runs as a repository-private, Unix-socket-only cluster
under ignored `.local/postgres/`; it does not touch an existing
`localhost:5432` service.

With the MBP-10 source under ignored `data/` and the machine-local ignored
`.env` configured, prepare and verify the complete environment with:

```bash
make research-ready
```

The first run streams every raw file for its full-content SHA-256; later runs
resume from file-identity checkpoints. Every row-level derivative, derived
Parquet snapshot, and market-data manifest stays below ignored
`data/derived/`.

Common session commands are:

```bash
make doctor
make catalog
make hash
make data-register
make qc
make qc-inspect
make qc-register
make smoke
make test
make notebook
make db-stop
```

See [`RESEARCH_ENVIRONMENT.md`](docs/RESEARCH_ENVIRONMENT.md) for setup,
configuration, command, PostgreSQL lifecycle, and recovery details.

Environment readiness is not research-data eligibility. All 1,434 source files
are fully hashed and registered as `HASHED` in PostgreSQL, but the dataset
remains `VALIDATING`. The DRAFT campaign contains 60 registered a-priori parent
experiments across six families, all with `pattern_id = NULL`; the pattern
ledger and trial ledger both remain empty. The one-day 1s/5m pilot is recorded
with checksum-backed database lineage, but its `VALIDATED` status covers only
structure and lineage and its metadata remains `research_eligible = false`.

The initial bounded source-qualification report and all market-data evidence
are under `data/derived/manifests/`. Its PostgreSQL record preserves three
`PASS`, one provider-partial `WARN`, and four then-current `FAIL` blockers; that
historical evidence is not rewritten when a later gate runs.

The full resumable structural scan has now completed over all 1,434 files,
49,846 row groups, and 3,220,073,651 rows. Its immutable final manifest SHA-256
is `47efca0afcd8ac97b78f3a5b7ce6b1194ef83d9764fa30314e253cbcbd296ffc`.
Only 1,428 files passed: six files failed with 11
`clean_trade_none_book_mutation` violations across 2024-06-30, 2024-07-01,
2024-07-14, 2026-04-19, 2026-06-07, and 2026-06-21. Therefore the structural
quality gate is `FAIL`, the dataset remains research-ineligible, and no strategy
performance may be interpreted from it.

A deterministic 11-row detail manifest under `data/derived/manifests/` shows
all pairs at exact `22:00Z`, but that clustering is not a semantic exemption.
It is linked in PostgreSQL as non-research artifact `29` / exposure `9`. The
frozen `phase1_structural_exclusions_v1` policy excludes all six source dates
pending status/MBO verification and forbids a hard-coded wall-clock exception;
the dataset-wide `FAIL` remains unchanged.

PostgreSQL now holds the 1,434 source checks plus structural and diagnostic
dataset checks. The aggregate is `FAIL`, diagnostics are `WARN`, and an
idempotent replay created nothing. Dataset `5` remains `VALIDATING`, all 1,434
sources remain `HASHED`, and the registry records `status_effect=NONE` and
`research_eligible=false`. Its canonical evidence also remains below
`data/derived/manifests/`.

Point-in-time instrument definitions and trading status, canonical expiry/roll
selection, resolution of the structural failures, the eligible calendar and
sealed split, research feature/outcome builds, and numeric cost and execution
assumptions are still blocking. See
[`DATA_SCHEMA.md`](docs/DATA_SCHEMA.md) for the authoritative gate status and
[`PROJECT_STRUCTURE.md`](docs/PROJECT_STRUCTURE.md) for storage and package
boundaries.

## Design documents

- [`Documentation index`](docs/README.md)
- [`System design`](docs/DESIGN.md)
- [`Research plan`](docs/RESEARCH_PLAN.md)
- [`Validation`](docs/VALIDATION.md)
- [`Research environment`](docs/RESEARCH_ENVIRONMENT.md)
- [`Research execution status`](docs/RESEARCH_EXECUTION_STATUS.md)
