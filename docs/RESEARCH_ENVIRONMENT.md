# Local Research Environment

- Status: workstation bootstrap verified
- Verified: 2026-08-03
- Runtime: uv-locked environment on Python 3.12.13
- Control plane: PostgreSQL 18.4, private Unix socket only
- Data contract: [`DATA_SCHEMA.md`](DATA_SCHEMA.md)

## 1. What “Ready” Means

The canonical environment bootstrap is:

```bash
make research-ready
```

It installs the exact locked dependency set, catalogs all Parquet footers, uses
a resumable scan to verify every raw file's full-content SHA-256, starts and
bootstraps the private PostgreSQL cluster, atomically registers the paired source
manifests, runs one bounded event smoke check, executes the test suite, and
finishes with the environment doctor. The workflow is safe to rerun: dependency
resolution stays locked, database migrations are checksum-verified, manifests
are deterministic, hash checkpoints are identity-checked, and source
registration rejects drift.

The long-running every-row-group structural scan is intentionally not part of
this default environment target. Its resumable implementation is invoked
explicitly with `make qc`, followed by `make qc-register` only after the final
manifest has been inspected. Both explicit steps have now completed; their
failed quality result is recorded below and does not alter `research-ready`.

An exit code of zero means the local software and control-plane environment are
ready for the next data-engineering stage. It does **not** mean that the full
market dataset is research-eligible or that strategy performance research may
start. Point-in-time `definition` and `status` references, contract/roll,
the failed every-row-group structural gate, calendar/split, research
feature/outcome, numeric cost, and numeric execution gates are tracked
separately in
[`DATA_SCHEMA.md`](DATA_SCHEMA.md#7-gate-status).

The workflow has been completed successfully on this workstation. PostgreSQL
may be stopped between sessions; a stopped cluster is expected and `make
research-ready` or `make db-up` starts it again.

## 2. Reproducible Python Runtime

The repository uses:

- `uv` from `PATH` as the environment and command runner (currently
  `/opt/homebrew/bin/uv` on this workstation)
- `uv.lock` as the exact dependency resolution for all extras
- `.python-version` to select the Python 3.12 line
- Python `3.12.13` as the currently verified interpreter
- `.venv/` as the ignored local virtual environment
- `.local/uv-cache/` as the ignored workspace-local uv cache

Every Make target executes project commands as:

```text
uv run --locked --all-extras ...
```

`--locked` prevents an ordinary research command from silently changing the
dependency graph. Change `pyproject.toml` and regenerate `uv.lock` deliberately
when dependencies need to change; do not bypass the lock to make a command
pass.

To install or restore only the Python environment:

```bash
make setup
```

## 3. Machine-Local Configuration

`Settings.from_env()` loads `<repository>/.env` without overriding variables
already supplied by the process. The real `.env` is ignored by Git and must
remain machine-local. `.env.example` documents the supported keys but is not a
live credential file.

The active local configuration owns these locations:

```text
SYSTEMATIC_FX_DATA_ROOT=<repository>/data
SYSTEMATIC_FX_ARTIFACTS_ROOT=<repository>/artifacts
SYSTEMATIC_FX_LOCAL_PG_ROOT=<repository>/.local/postgres
SYSTEMATIC_FX_PSQL=/Library/PostgreSQL/18/bin/psql
SYSTEMATIC_FX_PG_BIN=/Library/PostgreSQL/18/bin
SYSTEMATIC_FX_TEST_DATABASE_URL=<private socket URL ending in /systematic_fx_test>
SYSTEMATIC_FX_SMOKE_PARQUET=<repository>/data/mbp-10/2022/01/03/glbx-mdp3-20220103.mbp-10.parquet
```

`SYSTEMATIC_FX_DATA_ROOT/derived` is the mandatory location for every
row-level derivative, content-addressed derived Parquet snapshot, and
market-data lineage manifest. `SYSTEMATIC_FX_ARTIFACTS_ROOT` is only for compact
control-plane artifacts and reports; it must not become a second derived-data
root.

All database URLs use the private socket, not a TCP host:

```text
SYSTEMATIC_FX_DATABASE_URL=
  postgresql:///systematic_fx?host=%2F...%2F.local%2Fpostgres%2Fsocket&port=55432

SYSTEMATIC_FX_ADMIN_DATABASE_URL=
  postgresql:///postgres?host=%2F...%2F.local%2Fpostgres%2Fsocket&port=55432

SYSTEMATIC_FX_TEST_DATABASE_URL=
  postgresql:///systematic_fx_test?host=%2F...%2F.local%2Fpostgres%2Fsocket&port=55432
```

The `host` value is the percent-encoded absolute path to this checkout's
`.local/postgres/socket` directory. The local cluster uses socket-directory
permissions rather than a database password. Never commit a real `.env`, even
when it currently contains no password; paths, credentials, and connection
policy are machine-specific.

## 4. Private PostgreSQL 18 Cluster

The research control plane is independent of any PostgreSQL service already on
the workstation:

```text
.local/postgres/
├── data/                       # private PostgreSQL 18 data directory
├── socket/                     # mode 0700; Unix socket only
├── logs/postgresql.log
└── systematic_fx.conf          # generated safety configuration
```

Its enforced settings are:

```text
listen_addresses = ''
port = 55432
unix_socket_directories = <repository>/.local/postgres/socket
unix_socket_permissions = 0700
host authentication = reject
local socket authentication = trust
```

Port `55432` identifies the private PostgreSQL socket and keeps it distinct from
the default service. Because `listen_addresses` is empty, the project cluster
does not expose a TCP listener. Existing `localhost:5432` data, configuration,
processes, roles, and databases are not initialized, migrated, stopped, or
otherwise changed by these commands.

The entire `.local/` tree is ignored by Git. The lifecycle manager also refuses
unsafe, symlinked, broad, or non-`.local/postgres` data-directory targets before
running PostgreSQL tools.

## 5. PostgreSQL Lifecycle

The normal lifecycle is:

```bash
# Initialize if necessary, start, create systematic_fx if absent, and migrate.
make db-up

# Inspect without changing state.
make db-status

# Stop only this repository's private cluster.
make db-stop
```

The component commands remain available for diagnosis:

```bash
make db-init
make db-start
make db-bootstrap
make db-bootstrap-test
```

`db-bootstrap` connects first to the private `postgres` maintenance database,
creates `systematic_fx` only when absent, and applies the ordered migrations. It
does not create roles or change ownership of an existing database. Applied SQL
checksums are immutable; editing an applied migration is rejected.
`db-bootstrap-test` independently creates and migrates the fixed
`systematic_fx_test` database. Neither bootstrap accepts the other database's
URL, and no arbitrary database name is accepted.

## 6. Research Commands

### Complete readiness workflow

```bash
make research-ready
```

The target runs, in order:

```text
setup → catalog → hash → db-up → data-register → smoke
      → db-bootstrap-test → test → doctor
```

This is the dependency order realized by the Makefile: `research-ready`
depends on `data-register`, which depends on both `hash` (and therefore
`catalog`) and `db-up`; `test` depends on `db-bootstrap-test`. `hash` reads all
source bytes on its first successful run; an unchanged rerun uses the checkpoint
and still reproduces the canonical manifest.

### Environment diagnosis

```bash
make doctor
```

The Make target requires a live configured database. It checks Python,
scientific dependencies, data and artifact directories, `psql`, and PostgreSQL
connectivity. If the private cluster was stopped, run `make db-up` first.

### Footer catalog

```bash
make catalog
```

This validates every Parquet footer and rewrites the deterministic ignored
manifest at `data/derived/manifests/mbp10_footer_manifest_v1.jsonl`. It does not
scan the 3.22 billion event rows.

### Full-content hash and source registration

```bash
make hash
make data-register
```

`make hash` streams each raw Parquet file and writes the canonical ignored
manifest at `data/derived/manifests/mbp10_source_sha256_v1.jsonl`; it is not a
footer-only check. The completed manifest covers 1,434 files and
156,675,982,394 bytes and has SHA-256
`14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de`.

`make data-register` first satisfies `hash` and `db-up`, then verifies the
footer/hash manifests as an exact pair before one PostgreSQL transaction. The
current dataset is `VALIDATING` with 1,434 `HASHED` source rows. It is not
promoted to `READY` until later quality gates pass.

To persist the bounded checks and all current blockers:

```bash
uv run --locked --all-extras systematic-fx data qualify --json
```

The command writes canonical evidence only under `data/derived/manifests/` and
registers eight dataset-target checks. Its current exit code is `1` because the
report is successfully recorded but remains `BLOCKED`; rerunning is a no-op.
It never promotes the dataset or source rows.

### Full structural QC (complete, gate failed)

```bash
make qc
make qc-inspect
make qc-register
```

`make qc` first ensures the immutable source-hash manifest exists, then scans
every row group using `configs/data/mbp10_structural_qc_v1.toml`. Progress is
reported every 250 completed row groups by default; override only the reporting
interval with `QC_PROGRESS_EVERY=<positive integer>`. The durable checkpoint
and final manifest are confined to `data/derived/manifests/`, so an interrupted
run safely resumes without creating a second derived-data root.

The full run is now `COMPLETE`: 1,434 files, 49,846 row groups, and
3,220,073,651 rows were covered. The final manifest SHA-256 is
`47efca0afcd8ac97b78f3a5b7ce6b1194ef83d9764fa30314e253cbcbd296ffc`.
It reports 1,428 passing files and six failing files, with 11 hard
`clean_trade_none_book_mutation` observations:

```text
2024-06-30: 2    2024-07-01: 2    2024-07-14: 2
2026-04-19: 3    2026-06-07: 1    2026-06-21: 1
```

Accordingly, `make qc` returns `1` for a completed structural `FAIL` rather
than an execution error. Non-gating diagnostics remain in the manifest and do
not change that result; unsafe input, provenance drift, or execution errors
return `2`.

After the scan finishes, `make qc-register` verifies the scan against the
full-content source manifest and current PostgreSQL source identities. The
completed registration created 1,434 source checks plus dataset structural
check `1451` and diagnostic check `1452`. Its aggregate is `FAIL`, diagnostics
are `WARN`, artifact ID is `27`, and job ID is `22`. The immutable registration
evidence SHA-256 is
`adb894b20cf0e60819a31e18f9c64c74975ced18916eb4aefd9112ae4dc9c355`
under `data/derived/manifests/full_qc_registry_v1/sha256/`.

Registration preserved dataset ID `5` as `VALIDATING` and all 1,434 sources as
`HASHED`; `status_effect=NONE` and `research_eligible=false`. An immediate
replay created no artifact, job, evidence, or quality check, confirming
append-only idempotence. The structural `FAIL` and diagnostic `WARN` remain
separate; neither result permits research eligibility.

`make qc-inspect` deterministically replays the six failed sources and requires
the final-manifest count, unchanged scanner-helper count, and independently
located detail count to agree for every source. It accepts an existing output
only when the bytes are identical.

The 11-row, 39,567-byte local detail manifest
`data/derived/manifests/mbp10_clean_trade_none_book_mutations_v1.jsonl`
(SHA-256
`8f77077ae9243a2eedb893455a49bad1f75ff6ea02629a4ca442596e6ec67d23`)
supports investigation only. `phase1_structural_exclusions_v1.toml` freezes
whole-source-date exclusion for the six affected dates and prohibits a
hard-coded `22:00Z` exception until status/MBO evidence supports a new checker
version. PostgreSQL `EVENT_WINDOW` exposure `9` links the detail as artifact
`29`, with matching SHA-256 and `research_eligible=false`; its immediate replay
created nothing.

### Bounded event smoke

```bash
make smoke
```

This reads one row group from the fixed early-Discovery fixture. It verifies the
event schema and basic structural invariants only; it is not a whole-dataset
quality pass and cannot produce research evidence.

### Non-research pilot and governed registration

```bash
make pilot
uv run --locked --all-extras systematic-fx features register-pilot --help
uv run --locked --all-extras systematic-fx research register --help
uv run --locked --all-extras systematic-fx research exposure --help
```

`make pilot` is a bounded builder for explicit `6EH2` / provider instrument
`28727`; it has no active-contract or roll inference and refuses to overwrite
an existing output. All pilot Parquet files, content-addressed registry copies,
and the canonical lineage manifest remain under `data/derived/`.

The current pilot build and four AI-visible non-research exposures (source
summary, pipeline pilot, full-QC summary, and QC event-window detail) are
already registered.
PostgreSQL records the pilot's two derived partitions as `VALIDATED` for
**structure and lineage only**, with `research_eligible=false`. The DRAFT
campaign also has 60 a-priori experiments, 10 in each family P1-P6, all with a
null `pattern_id`; both the pattern and trial ledgers are still empty. These
commands are governance tools, not permission to calculate performance.

### Tests and lint

```bash
make test
make lint
```

Tests use committed/generated small fixtures and the configured local
boundaries. `tests/conftest.py` loads the ignored `.env` without overriding
process variables; it never infers the test database from the application URL.
Before any integration test runs, collection rejects a URL that does not
explicitly select `systematic_fx_test`. Tests do not perform an unbounded
full-event scan by default and cannot target the `systematic_fx` research
ledger through the supported workflow.

### Notebook

```bash
make notebook
```

This starts JupyterLab in the locked environment and remains in the foreground.
Run `make db-up` first when the notebook needs PostgreSQL. Notebook output is
exploratory only: accepted computations must be implemented in `src/`, tested,
and registered before they become research evidence.

### Stop after work

```bash
make db-stop
```

Stopping releases the private server process but preserves its ignored data
directory, migration history, and research control state for the next session.

## 7. Environment Status vs. Data Gates

These are intentionally different milestones:

| Boundary | Current status | Meaning |
|---|---|---|
| Locked Python/scientific runtime | PASS | Python 3.12.13 and locked extras import correctly |
| Private PostgreSQL bootstrap | PASS | PostgreSQL 18.4 migrations `0001`-`0007` cover separate `systematic_fx` research and `systematic_fx_test` integration databases, including immutable run provenance, governed Discovery exposure, and the publication outbox |
| Full footer catalog | PASS | All 1,434 footers satisfy the current raw contract |
| Full source SHA-256 manifest | PASS | All 1,434 files hashed; unchanged checkpoint rerun reproduced the manifest |
| Source control-plane registration | PASS | Dataset `VALIDATING`; all 1,434 sources `HASHED` |
| Bounded source qualification registry | RECORDED/BLOCKED | 3 PASS, 1 WARN, 4 FAIL; canonical evidence under `data/derived/manifests/` |
| Full every-row-group structural QC | FAIL / COMPLETE | 1,434 files and 49,846 row groups covered; 6 files contain 11 hard violations; 1,436 checks registered |
| Bounded event smoke | PASS | One controlled row group satisfies structural checks |
| A-priori hypothesis registration | PASS for governance only | 60 experiments; pattern ledger 0; trial ledger 0 |
| Non-research pilot lineage | PASS for structure/lineage only | Two derived partitions registered with `research_eligible=false` |
| Reference, roll, split, cost, and execution gates | PENDING | Definition/status/calendar and numeric cost/execution inputs remain unresolved |
| Automated tests and doctor | PASS | 148 tests plus 27 subtests; doctor has 0 required failures and 0 warnings |
| Full research-data eligibility | PENDING | Remaining gates in `DATA_SCHEMA.md` are incomplete |

Environment readiness authorizes work on the remaining data pipeline. It does
authorize source qualification, explicitly non-research pipeline pilots, and
a-priori hypothesis registration. It does not authorize performance-bearing
feature/outcome generation, AI pattern claims, backtest interpretation,
strategy promotion, Paper, or Live execution.

## 8. Recovery Checklist

If a local command fails:

1. Run `make db-status`.
2. Run `make db-up` if the cluster is stopped.
3. Run `make doctor` and address the first required failure.
4. Inspect `.local/postgres/logs/postgresql.log` for a server startup failure.
5. Confirm `.env` points to this repository's socket and port `55432`, not
   `localhost:5432`.
6. Run `make test` after correcting the environment.

Do not delete `.local/postgres`, regenerate `uv.lock`, or redirect commands to
another PostgreSQL instance merely to bypass a failed check.
