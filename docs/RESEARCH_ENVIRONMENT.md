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

### M0a finite-budget walking skeleton

M0a is the first complete daemon loop, but it is deliberately an engineering
fixture run rather than production performance research. The current full
dataset is still research-ineligible and the repository has no verified CME
session/status reference from which `NO_CROSS_CLOSED_MARKET` can be proven for
real rows. The checked-in fixture therefore models ordinary liquid sessions, a
previous-day-volume contract change, roll guards, Friday, and the session-close
boundary with explicit metadata and expected behavior. It also contains one
explicitly planted deterministic mechanics pattern so the skeleton exercises
raw-versus-flat occupancy, sequential replay, walk-forward, null controls, and
`REGISTER`; that fixture result is not market evidence or an alpha claim.

```bash
# Defaults: epochs/m0a_fixture_v1.toml and .local/m0a/
uv run systematic-fx research m0a build-features
uv run systematic-fx research m0a build-labels
uv run systematic-fx research m0a run-epoch
uv run systematic-fx research m0a daemon start --keep-alive
uv run systematic-fx research m0a report
uv run systematic-fx research m0a verify-invariants
```

The feature and label commands publish immutable content-addressed JSONL and
verify exact existing bytes on replay. `run-epoch` registers the manifest's
fixed 12 real and 24 null experiments, then processes or resumes the durable
queue. `daemon start` drains/resumes the finite epoch and exits when idle;
`--keep-alive` keeps the healthy process polling after generation is exhausted,
but it cannot enqueue beyond those budgets. Candidate errors are isolated; stale
leases become `CRASHED` and receive a new numbered attempt; an exact successful
rerun reopens the stored artifact. `report` writes an exploratory Markdown
report below `reports/generated/`; no output is Paper- or Live-eligible.

The SQLite ledger is intentionally local and Discovery-only. It remains the
M0a fixture authority. M0b migration `0029` adds a separate PostgreSQL
finite-budget search ledger for later governed real-data epochs; it does not
retroactively turn the M0a fixture into market evidence.

The public M0b registration boundary is
`systematic_fx.db.m0b_registry.register_m0b_candidate`, which inserts the
immutable CandidateWork artifact, RunSpec and budgeted candidate atomically.
Direct generic RunSpec registration is deliberately rejected for an M0b
campaign. The bounded `research m0b worker-cycle` command claims at most one
already-registered item. PostgreSQL returns the immutable CandidateWork hash
and byte size; the runner reopens that exact content-addressed local file,
verifies its signal and first-passage lineage, and publishes
checkpoints and the terminal classification through migration `0030`'s four
allowlisted capabilities. An owner-only durable lease token, heartbeat,
expired-attempt recovery and exact failure/terminal replay make a killed cycle
restart safe. The command cannot generate candidates, change an epoch, open a
holdout or promote a result. It is a finite operational worker cycle, not a
deployed autonomous performance epoch. A real eligible epoch still requires
official schedule/status coverage, point-in-time active-contract evidence and
an externally provisioned actual worker/holdout credential boundary.

### M0b real-slice bridge

`configs/data/cme_6e_reference_v1.toml` and
`configs/research/m0b_real_slice_v1.toml` freeze a four-source-file, three-
session bridge through the actual MBP-10 reader. The materializer reads only
the allowlisted contract and four-hour window, retains raw event order for
same-second TP/SL fallback, and publishes content-addressed quote-second,
feature, label, and build manifests. Passive TP needs an aggressor-side trade
through; executable quote touch alone is insufficient.

The observed 2022-08-31 volumes were U2 261,517 versus Z2 2,850. Therefore the
September 1 Z2 slice is explicitly
`CONTRACT_TRANSITION_CONTEXT_NOT_ACTIVE_SELECTION`, not a point-in-time active
contract claim. Every materialized label keeps `status_coverage=false` and
`entry_eligible=false`; mechanically valid outcomes carry
`SCHEDULE_ONLY_STATUS_UNVERIFIED`, because the CME schedule cannot prove an
unscheduled halt. These rows test mechanics and lineage only; they cannot enter
the search daemon as eligible trades.

The independent roll-context manifest
`configs/data/cme_6e_active_contract_roll_context_v1.toml` proves the
point-in-time `previous_completed_trading_date_volume_v1` mapping on an exact
four-file allowlist. It aggregates each complete CME trading session across
both intersecting UTC partitions rather than treating one UTC file as a
trading day. The immutable identities are:

- manifest bytes:
  `0df60badcfc0f191f22ca26b2d0ee6a439cb6c90b715580398577af8bcfc5b82`;
- normalized manifest semantics:
  `cfe0f62425f5cdb2e1d7687d1163de800334aca2881d5775a4f74b19ac2d5626`;
- materialized mapping artifact:
  `3092fdb96e5aba7e64ac41f051f670c7ae8d969323d00766d3b94032256220a0`.

The September 15 completed session contains U2 volume 158,500 versus Z2
62,531, so the mapping available for the September 16 session selects U2.
That fact does not authorize entry: U2 is already in its delivery roll guard,
and entry eligibility rejects it. The September 16 completed session contains
U2 volume 24,706 versus Z2 224,580, so the mapping frozen before the September
19 open selects Z2. `active_contract_mapping_as_of` refuses either result
before its evidence session has closed.

`CmeTradingStatusEvidence.status_at` is the separate archived-status lookup. It
rejects future-published and stale observations, wrong scope, and missing
temporal coverage; its `coverage_verified` flag says only that a timely
in-scope observation exists. The loader separately rejects protected paths and
ancestor symlinks, while full entry eligibility reopens the archive and its
upstream bytes and requires their exact identities plus a schedule-safe horizon
and entry-time OPEN status.
The repository contains only the explicitly opt-in deterministic fixture
`tests/fixtures/cme_trading_status_fixture_v1.toml` plus its separately hashed
test upstream bytes; it is marked
`TEST_ONLY_NOT_CME_EVIDENCE` and cannot be loaded without
`allow_test_fixture=true`. No actual archived CME status feed has been
fabricated, so the August real bridge remains correctly ineligible.

Long-horizon calendar coverage has the same evidence boundary. The existing
`cme_6e_reference_v1.toml` remains a narrow August/September mechanics
reference and is not stretched into a fabricated 2022--2026 holiday archive.
`CmeScheduleArchive` instead accepts exact session revisions, publication
timestamps, closes, and intra-session breaks; `session_as_of` selects only a
revision already published at the requested time. Coverage gaps, overlapping
sessions or breaks, future revisions, and missing archives fail closed. The
only checked-in archive is the explicitly synthetic test fixture with file
SHA-256
`6476edaaa819177dbcd3bc337bcc906d9d40e8a92d8777256c78b43ce3d55864`
and its test-only upstream source SHA-256 is
`1be333bdb00c3bcc53a6c94381e0e92f644c3ab3309300aee59664225e182292`;
there are no asserted official long-range CME schedule bytes yet.

These evidence boundaries are executable. A schedule archive is reopened with
its separately archived upstream bytes, and entry queries require schedule
knowledge exactly as of the event:

```bash
uv run systematic-fx research m0b verify-schedule-archive \
  --archive path/to/cme_schedule_archive.toml \
  --source path/to/archived_upstream_bytes

uv run systematic-fx research m0b verify-status-evidence \
  --evidence path/to/cme_status_archive.toml \
  --source path/to/archived_status_feed_bytes \
  --event-ts-ns 1662040000000000000
```

Active-contract evidence can resolve its previous completed session through
the same schedule archive, so a holiday is not approximated as an ordinary
weekday. The checked-in schedule/status files are deterministic fixtures and
require `--allow-test-fixture`; library entry authorization additionally
rejects them unless an explicit test-only switch is supplied. They exercise
mechanics but cannot authorize the currently nonexistent production epoch.
A schedule-safe horizon remains ineligible unless an independently verified,
already-observed `OPEN` status covers the entry instant.

```bash
uv run systematic-fx research m0b verify-active-contract-mapping \
  --manifest path/to/active_contract_manifest.toml \
  --schedule-archive path/to/cme_schedule_archive.toml \
  --schedule-source path/to/archived_upstream_bytes \
  --data-root path/to/search_only_raw_root
```

Without an archive, the command fails closed. The explicit
`--allow-bounded-weekday-fallback` option exists only for the checked-in,
holiday-free engineering fixture and is not production calendar evidence.

The bounded real label artifact can be converted without recomputation into
eight immutable, event-group-preserving first-passage shards. The current
checked-in identities are config bytes
`93a9120661b4c11a6a05e36c3d5ca24da3005918635c25a041179b61479da6d7`,
store spec
`bc3a1cb4ececf7a4960900778b576b09782f971cd98b0015e91599e81affa4fb`,
and reconstructed store
`18670ad2e98e5a18fb3ad7c18f2768e60c9ca2560e6a3dd7ac0e7dc2fe6f5ab4`.
Verification decodes every row and proves the shard concatenation equals the
original label SHA, feature lineage, version, global order and cardinality.

The operator stages those bytes before registering CandidateWork:

```bash
uv run systematic-fx research m0b materialize-real-slice \
  --output-root artifacts/research/m0b_real_slice_v1

uv run systematic-fx research m0b build-first-passage-store \
  --build artifacts/research/m0b_real_slice_v1/build-17f4ccdcb839c70bfdd95c9d00a2b37ca6d31fff89c34439a2adcaac4c32cf5f.json \
  --store-root artifacts/research/m0b_first_passage_store_v1

uv run systematic-fx research m0b verify-first-passage-store \
  --store artifacts/research/m0b_first_passage_store_v1/first-passage-store-18670ad2e98e5a18fb3ad7c18f2768e60c9ca2560e6a3dd7ac0e7dc2fe6f5ab4.json
```

After an operator has atomically registered CandidateWork and a frozen epoch,
one least-privilege cycle is:

```bash
SYSTEMATIC_FX_M0B_WORKER_DATABASE_URL='postgresql://...' \
SYSTEMATIC_FX_M0B_WORKER_DATABASE_USER='systematic_fx_m0b_worker_login' \
  uv run systematic-fx research m0b worker-cycle \
    --epoch-key '<frozen-epoch-key>' \
    --worker-id 'm0b-worker-1' \
    --worker-root artifacts/research/m0b_worker
```

Repeated invocations drain only the precommitted finite queue. A healthy idle
cycle exits without creating work; a missing or broader credential, changed
work bytes, stale lineage or classification disagreement fails closed.

The CLI-ready Python boundaries are
`materialize_active_contract_mapping_artifact` and
`verify_active_contract_mapping_artifact`. The actual bounded gate is:

```bash
SYSTEMATIC_FX_RUN_CME_ROLL_CONTEXT=1 \
  uv run pytest tests/integration/test_cme_active_contract_real.py -q -s
```

```bash
# Exact allowlist → content-addressed source/quote/feature/label/build bytes.
uv run systematic-fx research m0b materialize-real-slice

# Reopen the exact returned build filename without substitute discovery.
uv run systematic-fx research m0b verify-real-slice \
  --build artifacts/research/m0b_real_slice_v1/build-<sha256>.json

# Fresh disposable PostgreSQL 1..30 plus lifecycle and negative gates.
SYSTEMATIC_FX_RUN_M0B_PG_GATE=1 \
  uv run pytest tests/integration/test_m0b_control_plane_postgres.py \
  tests/integration/test_m0b_holdout_provisioning_postgres.py \
  tests/integration/test_m0b_worker_capability_postgres.py -q -s
```

### Sealed-holdout deployment boundary

The ordinary workstation PostgreSQL bootstrap does **not** provision a distinct
research daemon LOGIN credential. A logical `SEALED` marker alone is not a
SELECT permission boundary. M0a therefore fails closed at the process boundary:

- its manifest schema rejects holdout/sealed/credential/path keys;
- startup fails if any `SYSTEMATIC_FX_HOLDOUT_*` variable is present;
- only the staged M0a input-artifact root is opened;
- no holdout evaluator, unseal, promotion, Paper, Live, broker, or LLM API is
  imported by the daemon.

Production provisioning must put sealed bytes in a separate
storage namespace or database schema and issue distinct credentials. The
recommended roles are a DDL-owning `migration_admin`, a Discovery-only
`research_daemon`, and a separately provisioned `holdout_executor`. Revoke
schema/table/object-store read access from `research_daemon`, do not mount or
export holdout paths/tokens into that process, and verify the deployment with
`has_schema_privilege` / `has_table_privilege` plus an actual denied read. The
holdout executor must not be interactive or AI-accessible and must run only
after a separate immutable authorization. Until that external permission test
passes, reports must retain `UNTOUCHED_ACCESS_DENIED` and no holdout claim may
be made beyond the process boundary.

The holdout verifier rejects direct DML and executable privilege escalation for
the Discovery-only research credential. A separate M0b worker verifier accepts
exactly four migration-0030 `SECURITY DEFINER` capabilities while still
rejecting direct ledger DML and all sealed access. This is a tested
least-privilege mutation boundary, not authorization to open holdout data,
promote beyond REGISTER, or run without precommitted worker input.
The worker cannot read the lease table's bearer-token column; each lease is
also bound to the authenticated LOGIN. CandidateWork v2 and the final complete
checkpoint bind the exact executable barrier, evaluation policy, result hash,
byte size, integer metrics, and DB-derived classification before terminalization.

After provisioning a distinct worker LOGIN, verify that exact boundary with:

```bash
SYSTEMATIC_FX_M0B_WORKER_DATABASE_URL='postgresql://...' \
SYSTEMATIC_FX_M0B_WORKER_DATABASE_USER='systematic_fx_m0b_worker_login' \
  uv run systematic-fx db verify-m0b-worker-access --json
```

The repository now includes `deploy/postgres/provision_m0b_holdout.sql` and
`scripts/verify_m0b_holdout_isolation.py`. Provisioning creates separate
NOLOGIN group roles and revokes the research group from the sealed schema; the
verifier accepts only the daemon database URL and rejects superuser,
`BYPASSRLS`, direct or transitive `SET ROLE`, executor/owner membership,
declared privileges, or a successful direct read. Provisioning also refuses a
pre-existing sealed table whose owner, persistence, columns, defaults, or
constraints differ from the frozen shape. Deployment still has to create the
distinct LOGIN credential and object-store mount outside the repository. The
current local cluster uses a privileged session and trust authentication, so
its correct state is `NOT_PROVISIONED`, not a simulated PASS via `SET ROLE`.

```bash
# Run as migration administrator, never as the daemon.
psql -X --set=ON_ERROR_STOP=1 "$SYSTEMATIC_FX_ADMIN_DATABASE_URL" \
  --file deploy/postgres/provision_m0b_holdout.sql

# Run with the daemon's actual NOSUPERUSER/NOBYPASSRLS LOGIN URL.
SYSTEMATIC_FX_RESEARCH_DATABASE_URL=postgresql://... \
SYSTEMATIC_FX_RESEARCH_DATABASE_USER=research_login \
  uv run systematic-fx db verify-holdout-isolation --json
```

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

### Autonomous proposal-only AI research

The autonomous proposer is intentionally outside the M0b worker and PostgreSQL
mutation boundary. It receives one content-addressed Discovery-only bar-
morphology context, evaluates the exact 560-rule direction-consistent catalog
frozen in `configs/research/ai_pattern_discovery_v3.toml`, and publishes exactly
12 hypotheses to an append-only predecessor-hash ledger. It cannot inspect
labels, outcomes, PnL, walk-forward, or sealed holdout state, and it has no
database URL or promotion API.

```bash
make ai-pattern-run       # Batch 3 is allowed exactly once
make ai-pattern-verify    # read-only Batch 3 reconstruction
```

The current durable ignored root is
`data/derived/bar_patterns/ai_pattern_discovery_v3/`. The public command does
not accept another root, provider, budget, or threshold grid. A second `run` is
rejected instead of expanding the exposed search. `verify` does not create or
rewrite evidence; it reopens the approved 489-day source context, rebuilds the
106,605-row decision projection and 560-rule batch, and byte-compares all four
immutable artifacts. Batches 1 and 2 remain read-only replayable with `verify
--batch 1` and `verify --batch 2`. Batch 1 is superseded because its v1 catalog
contained 60 directionless range rules. Batch 2 fixed that rule semantics but
is superseded because its pinned Git commit did not contain its three new v2
runtime modules. Batch 3 closes that provenance gap by byte-comparing the full
`src/systematic_fx/**/*.py` tree, `pyproject.toml`, and `uv.lock` against commit
`686070e3f22891aa41ed75e432ca9c461ad14a1d` before context access. The generated
Markdown is a convenience view only; canonical JSON and the ledger are the
evidence.

The completed batch status is
`HYPOTHESES_GENERATED_AWAITING_ELIGIBLE_DATA`. It is a research start at the
hypothesis-generation stage, not a performance run. The M0b worker remains
idle for these proposals until research-eligible schedule/status/active-
contract coverage and a separately frozen label/null/evaluation epoch exist.

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
| Private PostgreSQL bootstrap | PASS | Fresh disposable gates apply the contiguous `0001`-`0030` chain; the persistent workstation database intentionally remains at `0028` |
| Full footer catalog | PASS | All 1,434 footers satisfy the current raw contract |
| Full source SHA-256 manifest | PASS | All 1,434 files hashed; unchanged checkpoint rerun reproduced the manifest |
| Source control-plane registration | PASS | Dataset `VALIDATING`; all 1,434 sources `HASHED` |
| Bounded source qualification registry | RECORDED/BLOCKED | 3 PASS, 1 WARN, 4 FAIL; canonical evidence under `data/derived/manifests/` |
| Full every-row-group structural QC | FAIL / COMPLETE | 1,434 files and 49,846 row groups covered; 6 files contain 11 hard violations; 1,436 checks registered |
| Bounded event smoke | PASS | One controlled row group satisfies structural checks |
| A-priori hypothesis registration | PASS for governance only | 60 experiments; pattern ledger 0; trial ledger 0 |
| Non-research pilot lineage | PASS for structure/lineage only | Two derived partitions registered with `research_eligible=false` |
| Reference, roll, split, cost, and execution gates | PENDING | Definition/status/calendar and numeric cost/execution inputs remain unresolved |
| Automated tests and doctor | PASS | The full unit suite and named M0b/CME/PostgreSQL gates pass; doctor has 0 required failures and 0 warnings |
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
