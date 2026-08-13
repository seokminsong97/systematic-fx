# Project Structure

- Status: implementation baseline
- Verified against local data and environment: 2026-08-03
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
├── .env                        # ignored: machine paths and database URLs
├── .local/                     # ignored: PostgreSQL state and uv cache
│   └── postgres/               # private Unix-socket-only PostgreSQL 18
├── .python-version             # selects the Python 3.12 runtime line
├── .venv/                      # ignored: uv-managed environment
├── Makefile                    # canonical setup, check, and lifecycle commands
├── configs/                    # versioned campaign/feature/cost/execution inputs
├── epochs/                     # immutable finite-budget research epoch manifests
├── data/                       # ignored: all raw, reference, and derived market data
│   ├── mbp-10/                 # immutable daily source files (existing layout)
│   ├── reference/              # point-in-time definition/status inputs (pending)
│   └── derived/                # features, research rows, outcomes, manifests
├── docs/                       # design, data-contract, and environment documents
│   └── phases/                 # Phase 1-5 implementation contracts
├── migrations/                 # checksum-verified ordered PostgreSQL SQL
├── reports/
│   └── generated/              # ignored: rendered research output
├── artifacts/                  # ignored: run manifests and frozen artifacts
├── src/systematic_fx/
│   ├── config/                 # environment-owned paths and credentials
│   ├── data/                   # inventory, mappings, contracts, and quality gates
│   ├── features/               # one-second and closed five-minute builders
│   ├── research/               # bounded research engines, ledgers, registration
│   │   ├── ai_*.py             # feature-only autonomous proposer and replay boundary
│   │   ├── m0a/                # deterministic Discovery-only walking skeleton
│   │   └── m0b/                # real bridge, first-passage shards, bounded worker
│   ├── strategies/             # immutable executable bracket policies
│   ├── backtest/               # event replay, fills, OCO, costs, and metrics
│   ├── validation/             # splitter, walk-forward, stress, and holdout
│   ├── db/                     # migration, bootstrap, and private-cluster control
│   ├── environment.py          # deterministic local-readiness checks
│   └── cli.py                  # composition root; no domain logic
├── tests/
    ├── unit/                   # pure and fast
    ├── integration/            # tiny Parquet/PostgreSQL boundaries
    ├── golden/                 # exact event-replay expectations
    └── leakage/                # point-in-time and split-isolation invariants
└── uv.lock                     # exact dependency resolution for all extras
```

Phase 2 broker adapters and Live execution packages are intentionally absent.
They should be added only after Phase 1 produces an eligible strategy and the
provider contracts are measured.

The M0a daemon uses an ignored local SQLite/WAL ledger and content-addressed
JSONL inputs below `.local/m0a/`. This is an engineering walking skeleton, not
governed performance evidence in PostgreSQL. M0b adds migrations `0029` and
`0030` for finite-budget search evidence and a least-privilege worker API,
plus a bounded real-data adapter and immutable first-passage store. The current
real rows stay non-entry-eligible until trading-status coverage exists.

## 3. Storage Boundaries

### Parquet

Parquet remains the data plane for raw events and large derived tables:

```text
data/
├── mbp-10/                     # immutable source; never rewritten
└── derived/
    ├── features_1s/version=.../contract=.../source_date=.../
    ├── research_5m/version=.../contract=.../source_date=.../
    ├── outcomes/version=.../contract=.../source_date=.../
    ├── registry/               # content-addressed derived Parquet snapshots
    └── manifests/              # source/build/quality/lineage evidence and QC checkpoints
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

The workstation cluster lives under ignored `.local/postgres/` and
uses only its mode-`0700` Unix socket with PostgreSQL port identifier `55432`.
It enforces `listen_addresses = ''`, so it has no TCP listener and does not
alter or compete with an existing `localhost:5432` PostgreSQL service. Its
ignored `.env` URLs select the socket explicitly. The `systematic_fx` research
database and `systematic_fx_test` integration database are fixed, separate
targets. Lifecycle and recovery are defined in
[`RESEARCH_ENVIRONMENT.md`](RESEARCH_ENVIRONMENT.md).

### Artifacts

`artifacts/` holds compact control-plane outputs such as registration records,
AI context packages, frozen strategy JSON, trade-ledger references, and
validation bundles. Market-data manifests and all row-level derivatives remain
under `data/derived/`. The database records artifact identities and checksums;
generated files remain out of Git.

## 4. Package Dependency Rules

- `config` depends on the standard library and `python-dotenv`; process
  variables take precedence over the ignored repository `.env`.
- `data` owns external file formats, metadata normalization, and quality gates.
- `features` may consume validated `data`; it cannot inspect future events.
- `strategies` defines policies without depending on a backtest implementation.
- `research` consumes feature summaries and emits registered strategy inputs.
- `research.m0a` may use only its explicitly staged Discovery fixture/input
  artifacts. It cannot import holdout credentials, invoke an LLM, or call a
  broker/promotion package.
- `research.m0b` may open only the exact source allowlist and output root in its
  immutable manifest. It treats the CME schedule and trading-status feed as
  separate contracts and cannot assert active selection or entry eligibility
  when either proof is missing.
- `backtest` consumes validated events and frozen strategies; it cannot mutate
  either.
- `validation` orchestrates frozen backtests over deterministic splits.
- `db` persists identities and state through explicit repository interfaces.
- `cli.py` wires packages together and is the only layer allowed to know all of
  them.

No package may import from `tests`, generated artifacts, or notebooks. Notebook
results are exploration only; every accepted computation must exist in `src/`
and be registered.

## 5. Current Runnable Boundaries

The canonical locked workflow is:

```bash
make research-ready
```

It resolves the checked-in `uv.lock` with Python 3.12.13, bootstraps the private
PostgreSQL control plane, catalogs every footer, scans one bounded event row
group, runs tests, and requires the environment doctor to pass. Individual
boundaries are available through:

```bash
make doctor
make catalog
make hash
make data-register
make qc
make qc-register
make smoke
make test
make notebook
make db-stop
```

The exact 73-column contract, all 1,434 footers and source checksums,
instrument-kind mappings, source registration, bounded qualification ledger,
the resumable every-row-group QC and append-only registration interfaces, the
early-Discovery pipeline pilot, 60 a-priori hypotheses, and PostgreSQL 18.4
control plane are implemented and tested. The explicit `qc` then `qc-register`
flow has completed outside `research-ready`: it covered all 1,434 files, 49,846
row groups, and 3,220,073,651 rows, then registered 1,434 source checks plus two
dataset checks. The result is `FAIL`, not eligibility: six source files contain
11 `clean_trade_none_book_mutation` violations; diagnostics are separately
`WARN`. Dataset `5` remains `VALIDATING`, all sources remain `HASHED`, and an
immediate registration replay created nothing. The scan-manifest SHA-256 is
`47efca0afcd8ac97b78f3a5b7ce6b1194ef83d9764fa30314e253cbcbd296ffc`.
Every checkpoint, scan manifest, and registration-evidence file remains under
`data/derived/manifests/`. See
[`DATA_SCHEMA.md`](DATA_SCHEMA.md) for data-gate status and
[`RESEARCH_ENVIRONMENT.md`](RESEARCH_ENVIRONMENT.md) for command semantics.

Environment bootstrap and full research-data eligibility are separate states.
The environment is ready, but the dataset cannot yet be used for interpretable
strategy results.

The next implementation slice proceeds in order:

1. Resolve point-in-time definition/status inputs, verified expiry metadata,
   and contemporaneous execution-contract/roll state; spread mappings remain
   non-executable.
2. Classify and resolve the six failed source files through a governed new
   source or semantic version; do not weaken or overwrite the zero-tolerance
   structural result.
3. Freeze the eligible-day calendar and deterministic sealed campaign split.
4. Freeze numeric cost and execution models.
5. Build versioned one-second features, five-minute research rows, and
   event-ordered outcomes.

Performance-bearing feature generation must not begin before steps 1-4 make the input universe,
execution contracts, and sealed periods explicit.
