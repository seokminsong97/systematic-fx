# Project Structure

- Status: implementation baseline
- Verified against local data: 2026-08-02
- Governing design: [`DESIGN.md`](DESIGN.md)

## 1. Decisions From the Actual Dataset

The local source currently contains 1,434 daily Parquet files (145.92GiB of
logical file data and about 154GiB on disk)
from `2022-01-02` through `2026-07-31`:

```text
data/mbp-10/YYYY/MM/DD/glbx-mdp3-YYYYMMDD.mbp-10.parquet
```

The raw layout stays in place; moving 154GB provides no research benefit.
`data/` is ignored as one machine-local storage boundary.

The inspected Parquet metadata establishes several non-negotiable ingestion
rules:

- Dataset/schema are `GLBX.MDP3` and `mbp-10`.
- Prices are fixed-encoded `int64` values with a `1e-9` scale.
- The undefined-price sentinel is `9223372036854775807`.
- The parent symbol is `6E.FUT`, and daily metadata includes both outright
  contracts and calendar spreads.
- Research and execution must resolve `instrument_id` through each file's
  Databento mappings and explicitly reject spreads as execution instruments.

File date is source partition metadata, not automatically an exchange trading
session date. Session and roll assignment belong in the verified contract
normalization layer.

## 2. Repository Layout

```text
systematic-fx/
├── configs/                    # versioned campaign/feature/cost/execution inputs
├── data/                       # ignored: raw and derived Parquet datasets
│   └── mbp-10/                 # immutable daily source files (existing layout)
├── docs/                       # governing, research, validation, and phase designs
│   └── phases/                 # Phase 1-5 implementation contracts
├── migrations/                 # Alembic history for PostgreSQL metadata
├── reports/
│   └── generated/              # ignored: rendered research output
├── artifacts/                  # ignored: run manifests and frozen artifacts
├── src/systematic_fx/
│   ├── config/                 # environment-owned paths and credentials
│   ├── data/                   # inventory, mappings, contracts, and quality gates
│   ├── features/               # one-second and closed five-minute builders
│   ├── research/               # AI packages, ledgers, experiment registration
│   ├── strategies/             # immutable executable bracket policies
│   ├── backtest/               # event replay, fills, OCO, costs, and metrics
│   ├── validation/             # splitter, walk-forward, stress, and holdout
│   ├── db/                     # PostgreSQL models and repositories
│   └── cli.py                  # composition root; no domain logic
└── tests/
    ├── unit/                   # pure and fast
    ├── integration/            # tiny Parquet/PostgreSQL boundaries
    ├── golden/                 # exact event-replay expectations
    └── leakage/                # point-in-time and split-isolation invariants
```

Phase 2 broker adapters and Live execution packages are intentionally absent.
They should be added only after Phase 1 produces an eligible strategy and the
provider contracts are measured.

## 3. Storage Boundaries

### Parquet

Parquet remains the data plane for raw events and large derived tables:

```text
data/
├── mbp-10/                     # immutable source; never rewritten
└── derived/
    ├── features_1s/version=.../contract=.../source_date=.../
    ├── research_5m/version=.../contract=.../source_date=.../
    └── outcomes/version=.../contract=.../source_date=.../
```

Every derived partition must carry its source checksums, code commit, feature
version, and build manifest. A new semantic definition creates a new version;
it does not overwrite an old partition.

### PostgreSQL 18.4

PostgreSQL is the control plane. Store:

- source-file catalog, checksums, footer metadata, and quality decisions
- instrument mappings, outright/spread classification, expiry, and roll state
- campaign definitions, eligible days, folds, purge windows, and holdout seals
- experiment lineage, pattern ledger, strategies, and parameter trial counts
- jobs, code/config versions, artifact locations, run status, and failures
- compact backtest/validation metrics and Paper-eligibility decisions

Do not copy event rows or wide one-second/five-minute feature tables into
PostgreSQL. Store their immutable Parquet URI and checksum instead.

### Artifacts

`artifacts/` holds reproducible local outputs such as manifests, AI context
packages, frozen strategy JSON, trade ledgers, and validation bundles. The
database records their identity and checksum. Large generated files remain out
of Git.

## 4. Package Dependency Rules

- `config` depends only on the standard library.
- `data` owns external file formats, metadata normalization, and quality gates.
- `features` may consume validated `data`; it cannot inspect future events.
- `strategies` defines policies without depending on a backtest implementation.
- `research` consumes feature summaries and emits registered strategy inputs.
- `backtest` consumes validated events and frozen strategies; it cannot mutate
  either.
- `validation` orchestrates frozen backtests over deterministic splits.
- `db` persists identities and state through explicit repository interfaces.
- `cli.py` wires packages together and is the only layer allowed to know all of
  them.

No package may import from `tests`, generated artifacts, or notebooks. Notebook
results are exploration only; every accepted computation must exist in `src/`
and be registered.

## 5. First Runnable Boundary

The initial command scans only filenames and file sizes; it does not load 154GB
of event rows or expose sealed market data:

```bash
PYTHONPATH=src python3.12 -m systematic_fx data inventory
PYTHONPATH=src python3.12 -m systematic_fx data inventory --json
```

The next implementation slice should extend this boundary in order:

1. Read every Parquet footer and register source checksums/mappings.
2. Classify outright versus spread instruments and build actual-expiry sessions.
3. Run schema, sentinel, ordering, sequence, and book-validity quality gates.
4. Freeze the eligible-day calendar and deterministic campaign split.
5. Build versioned one-second features, then five-minute research rows.

Feature generation must not begin before steps 1-4 make the input universe and
sealed periods explicit.
