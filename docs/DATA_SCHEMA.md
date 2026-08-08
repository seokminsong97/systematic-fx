# Phase 1 Data Schema and Bootstrap

- Status: executable ingestion baseline
- Verified: 2026-08-03
- Governing documents: [`DESIGN.md`](DESIGN.md),
  [`RESEARCH_PLAN.md`](RESEARCH_PLAN.md), [`VALIDATION.md`](VALIDATION.md)
- Runtime operations: [`RESEARCH_ENVIRONMENT.md`](RESEARCH_ENVIRONMENT.md)

## 1. Verified Raw Contract

Every source file must match one immutable Arrow contract before any event row
may enter feature generation:

- Dataset/schema: `GLBX.MDP3` / `mbp-10`
- Columns: exactly 73, in canonical DBN order
- Timestamps: `timestamp[ns, tz=UTC]`
- Prices: fixed `int64`, scale `1e-9`
- Undefined price: `9223372036854775807`
- Book: bid/ask price, size, and order count for levels 0 through 9
- Symbol mapping: `dbn.metadata` with `stype_out = instrument_id`

The canonical schema fingerprint is:

```text
57c7cc404aec87845b9e3872a4b2abcc651bd07858810324b4c9e3aa636ef5ea
```

This fingerprint covers ordered field names, Arrow types, nullability, and the
immutable price/DBN contract. It excludes daily instrument mappings, which are
expected to change.

## 2. Full Footer Catalog Result

The footer-only scan reads no event rows and completed over the entire local
dataset:

```text
files:                       1,434
logical bytes:               156,675,982,394
event rows:                  3,220,073,651
row groups:                  49,846
mapping intervals:           103,793
outright mappings:           35,131
calendar-spread mappings:    68,662
unknown mappings:            0
unique provider IDs:         518
schema fingerprints:         1
source range:                2022-01-02 through 2026-07-31
```

All files requested `6E.FUT`. No `not_found` symbols were reported. Provider
metadata reported 6,342 `partial` symbols across 795 files. A partial parent
response is retained as a warning; the selected execution outright must still
be checked separately for every eligible day.

The generated local footer manifest is:

```text
data/derived/manifests/mbp10_footer_manifest_v1.jsonl
SHA-256: 1451620c2f6c6a47a37cdc61d404e92795decbffaf48a24de99f4ff3c43a8633
```

It contains relative source paths, file/footer sizes, row counts, the schema
fingerprint, provider status lists, and each end-exclusive instrument mapping
interval. The ignored `data/` tree is the canonical home for both raw and
derived market-data manifests.

The resumable full-content scan subsequently hashed all 1,434 Parquet files:

```text
data/derived/manifests/mbp10_source_sha256_v1.jsonl
SHA-256: 14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de
source bytes: 156,675,982,394
```

A second run resumed all 1,434 checkpoint identities, hashed zero files again,
and reproduced the same manifest digest.

## 3. Instrument Boundary

Raw symbols are classified before research:

```text
6EH7          -> outright
6EU7-6EH7     -> calendar_spread
anything else -> unknown
```

Only verified outright contracts may become execution instruments. Calendar
spreads remain available for roll diagnostics but cannot provide signals,
fills, or first-touch outcomes. A provider `instrument_id` is meaningful only
inside its recorded mapping interval; never join it globally without the
source date.

## 4. PostgreSQL Control Plane

Migrations `0001`-`0007` create schema `systematic_fx` and its research-control
and publication-outbox tables, grouped by responsibility:

- Source/catalog: `datasets`, `source_files`, `instruments`,
  `instrument_mappings`, `quality_checks`
- Lineage: `derived_partitions`, `derived_partition_sources`, `artifacts`,
  `jobs`
- Validation calendar: `campaigns`, `campaign_splits`, `campaign_days`
- Research governance: `pattern_ledger`, `experiments`, `experiment_trials`,
  `discovery_exposures`
- Executable evidence: `strategies`, `backtest_runs`, `backtest_metrics`
- Public projection signal: `publication_outbox`
- Migration integrity: `schema_migrations`

The migration runner records the SQL SHA-256, refuses edited or missing applied
history, and skips an identical second application. Raw events and wide feature
rows remain in Parquet; PostgreSQL stores only control state, identities,
lineage, and compact results.

The verified workstation instance is PostgreSQL 18.4 under ignored
`.local/postgres/`. It uses a private mode-`0700` Unix socket and port identifier
`55432`, with `listen_addresses = ''` and host authentication rejected. It does
not connect to, migrate, stop, or alter an existing `localhost:5432` service.
The ignored `.env` selects this socket for the `postgres` maintenance database,
the `systematic_fx` research database, and the isolated `systematic_fx_test`
integration database.

The paired footer/content manifests have been registered under dataset key
`glbx_mdp3_mbp_10_6e_fut_v1`. Its database status is `VALIDATING`, and all
1,434 `source_files` are `HASHED`. The completed structural scan failed its
zero-tolerance gate, so it does not authorize promotion to `VALIDATED` or
`READY`. Source registration is idempotent and rejects manifest, URI,
byte-size, row-count, schema, or checksum drift without demoting an already
stronger state.

Bounded source qualification is persisted separately from promotion. The
canonical evidence file is
`data/derived/manifests/mbp10_source_qualification_v1.json` (SHA-256
`8f0c7c090c768a48e8cd7e788816689ee503082f8c53169340f813777632ebf9`).
The initial bounded qualification registration has eight dataset-target
`quality_checks`: footer identity, full hash identity, and mapping
classification are `PASS`; provider partial metadata is `WARN`; missing
definition/status references, the eligible calendar, and the then-pending full
row-group scan are four explicit `FAIL` blockers. This is immutable historical
evidence and is not rewritten to imply that the later scan passed. The artifact
is marked `research_eligible=false`, and registration leaves dataset/source
states unchanged.

The frozen `configs/data/mbp10_structural_qc_v1.toml` scanner subsequently
completed across the entire source set:

```text
final manifest:  data/derived/manifests/mbp10_structural_qc_v1.jsonl
SHA-256:         47efca0afcd8ac97b78f3a5b7ce6b1194ef83d9764fa30314e253cbcbd296ffc
files:           1,434 (1,428 PASS; 6 FAIL)
row groups:      49,846
rows:            3,220,073,651
hard violations: 11 (all clean_trade_none_book_mutation)
gate result:     FAIL
```

The failing source dates and hard counts are:

| Source date | Violations |
|---|---:|
| 2024-06-30 | 2 |
| 2024-07-01 | 2 |
| 2024-07-14 | 2 |
| 2026-04-19 | 3 |
| 2026-06-07 | 1 |
| 2026-06-21 | 1 |

`clean_trade_none_book_mutation` means a clean `T` or `N` row changed the
ten-level book relative to the preceding physical row for the same instrument
while the scanner held valid comparable book state. Zero tolerance is frozen,
so these 11 observations make the structural gate fail; they must be explained
or corrected through a new governed source/semantic version, not silently
ignored.

The performance-free detail manifest expands every hard count into exact
previous/current same-instrument row evidence:

```text
data/derived/manifests/mbp10_clean_trade_none_book_mutations_v1.jsonl
SHA-256: 8f77077ae9243a2eedb893455a49bad1f75ff6ea02629a4ca442596e6ec67d23
lines: 11
bytes: 39,567
```

All 11 pairs have previous and current `action=T`, `side=N`, and `flags=0`, and
all carry exact `ts_event=22:00:00Z`. Ten books transition from crossed to
normal and one from locked to normal, changing between 5 and 56 of the 60 book
fields. That timing concentration does not satisfy the frozen semantics: the
rows contain no snapshot, reset, or bad-book marker that authorizes a book
replacement, so the official interpretation remains a hard violation.

[`phase1_structural_exclusions_v1.toml`](../configs/data/phase1_structural_exclusions_v1.toml)
freezes the conservative interim policy. Each of the six complete source dates
is excluded from the future campaign-common eligible calendar until
point-in-time `status` and matching book reconstruction, using MBO when needed,
support a new checker version. A hard-coded `22:00Z` exception and implicit
reclassification are prohibited. Exclusion prevents contaminated use; it is
not a repair, a pass, or a promotion. The complete dataset remains `FAIL` and
research-ineligible.

The detail is reproduced by `data inspect-qc-mutations`, which requires the
manifest, unmodified scanner-helper, and independent detail counts to match per
source and refuses content drift at the immutable output path. PostgreSQL
`EVENT_WINDOW` exposure `9` links the JSONL as artifact `29` with the same
SHA-256 and `research_eligible=false`; an immediate replay created nothing.

The scanner also checks the exact schema, publisher/rtype/action/side/depth
domains, daily instrument mapping, request-range receive timestamps, trusted
per-instrument receive ordering, reset shape, sentinel/size/count consistency,
contiguous book levels, and strict bid/ask ladders. Receive/event clock
differences, raw sequence behavior, snapshot/bad-book flags, and locked/crossed
or incomplete BBOs remain diagnostics because they require later
status/session/reset-aware interpretation.

Both the durable checkpoint and final file-level JSONL remain only below
`data/derived/manifests/`. The completed append-only registration created 1,434
source checks plus dataset structural check `1451` and diagnostic check `1452`
(1,436 total). Aggregate structural status is `FAIL`; diagnostics are `WARN`.
Artifact `27` and job `22` reference canonical evidence with SHA-256
`adb894b20cf0e60819a31e18f9c64c74975ced18916eb4aefd9112ae4dc9c355`
at
`data/derived/manifests/full_qc_registry_v1/sha256/ad/adb894b20cf0e60819a31e18f9c64c74975ced18916eb4aefd9112ae4dc9c355.json`.

Registration cannot convert the result to eligibility: dataset `5` remains
`VALIDATING`, all 1,434 sources remain `HASHED`, `status_effect=NONE`, and
`research_eligible=false`. An immediate replay created zero evidence files,
artifacts, jobs, or checks, confirming idempotence. Definition, status, roll,
eligible-calendar, sealed split, numeric cost, and numeric execution gates also
remain open.

The governed DRAFT campaign is also registered without performance data:

```text
campaigns:          1  (phase1_discovery_v1, DRAFT)
experiments:       60  (P1-P6: 10 each; all pattern_id NULL)
pattern_ledger:     0
experiment_trials: 0
discovery_exposures: 4 (source SUMMARY, PIPELINE_PILOT, full-QC SUMMARY, QC EVENT_WINDOW; all research-ineligible)
```

## 5. Implemented Pilot vs. Pending Research Contract

[`mbp10_pilot_v1.toml`](../configs/features/mbp10_pilot_v1.toml) is the exact
implemented non-research feature contract. It requires an explicit active
outright symbol and provider instrument ID, uses `ts_recv` and right-closed
buckets, refuses to rewrite a closed second, emits only observed seconds, and
does not invent trading status, roll selection, labels, or performance. It
produced the following 2022-01-03 pilot:

```text
source:              6EH2 / provider instrument 28727
source SHA-256:      f682bae3b618f8905a76be1ff144a49620aea510ae3828ef245ffc0a53c7c2f8
selected events:     1,158,550
FEATURES_1S rows:    48,540
RESEARCH_5M rows:    281
valid 5m windows:    1
research_eligible:   false
```

The original versioned outputs are below the required ignored derived root:

```text
data/derived/features_1s/version=mbp10_pilot_v1/contract=6EH2/source_date=2022-01-03/
data/derived/research_5m/version=mbp10_pilot_v1/contract=6EH2/source_date=2022-01-03/
```

Before database registration, the files are re-read and checked against their
reported SHA-256, row counts, exact Arrow schemas, bucket bounds, date,
contract, provider ID, feature version, and `research_eligible=false` identity.
The registry then publishes immutable, content-addressed copies below
`data/derived/registry/pilot_v1/` and a canonical lineage manifest below
`data/derived/manifests/pilot_derived_registry_v1/`. PostgreSQL records one
succeeded build job, one lineage artifact, two `VALIDATED` derived partitions,
and two exact source links. This `VALIDATED` state means structure and lineage
only; it is not research-data eligibility.

The wider [`mbp10_v1.toml`](../configs/features/mbp10_v1.toml) describes the
intended research contract, including:

- `ts_recv` as the availability clock
- right-closed one-second buckets
- no rewriting of closed buckets by late events
- no forward fill across session boundaries or book resets
- L1/L3/L5/L10 book features and validity state
- trade/quote flow features
- closed five-minute distribution and path summaries

It has not been built or qualified. In particular, the final implementation
still requires point-in-time instrument definitions and trading status,
canonical roll selection, reset/staleness semantics, the common eligible
calendar, and the sealed split. Its future Parquet paths remain versioned:

```text
data/derived/features_1s/version=mbp10_v1/contract=.../source_date=.../
data/derived/research_5m/version=mbp10_v1/contract=.../source_date=.../
data/derived/outcomes/version=.../contract=.../source_date=.../
```

## 6. Commands

The canonical all-in-one environment workflow is:

```bash
make research-ready
```

The data boundaries can be rerun independently:

```bash
# Full footer contract and deterministic manifest; no event-row scan.
make catalog

# Resumable full-content SHA-256 over every raw file.
make hash

# Verify the paired manifests and atomically register 1,434 source rows.
make data-register

# Record all bounded qualification passes, warnings, and blockers.
# Exit 1 currently means the evidence was recorded but blockers remain.
uv run --locked --all-extras systematic-fx data qualify --json

# Explicit long-running full structural scan; safely resumes its row-group checkpoint.
make qc

# After inspecting the immutable final manifest, register 1,434 + 2 checks.
make qc-register

# One bounded event-row-group structural smoke test.
make smoke

# Build the explicit one-day non-research pilot on a clean output path only.
# Existing pilot outputs are intentionally never overwritten.
make pilot

# Locked tests and environment checks against the running private database.
make test
make doctor

# Exploratory JupyterLab session, then stop the private database after work.
make notebook
make db-stop
```

See [`RESEARCH_ENVIRONMENT.md`](RESEARCH_ENVIRONMENT.md) before changing paths,
database URLs, dependency resolution, or PostgreSQL lifecycle state.

## 7. Gate Status

### Environment readiness

The uv-locked Python 3.12.13 runtime, required scientific imports, private
PostgreSQL 18.4 bootstrap, migrations, full footer catalog, bounded smoke test,
automated tests, and database-required doctor have been verified. This is an
operational readiness result only.

### Research-data eligibility

| Gate | Status |
|---|---|
| Exact 73-column Arrow/DBN contract | PASS |
| All footer dates and `YYYY/MM/DD` partitions | PASS |
| One schema fingerprint across 1,434 files | PASS |
| Outright/spread/unknown mapping classification | PASS |
| Bounded 2022-01-03 event structural smoke | PASS |
| PostgreSQL 18.4 migration and constraints | PASS on separate workspace-private research/test databases |
| Per-file full-content SHA-256 | PASS: 1,434/1,434; resumable rerun reproduced manifest |
| PostgreSQL source manifest registration | PASS: dataset `VALIDATING`; 1,434/1,434 files `HASHED` |
| Bounded source qualification registry | RECORDED: 3 PASS, 1 WARN, 4 FAIL; evidence is research-ineligible |
| A-priori campaign registration | PASS for governance only: 60 experiments, 0 patterns, 0 trials |
| Point-in-time instrument `definition` reference | PENDING |
| Point-in-time trading `status` reference | PENDING |
| Canonical contract expiry and roll selection | PENDING |
| Every-row-group structural scan | FAIL: COMPLETE coverage; 1,428 PASS / 6 FAIL files; 11 hard violations |
| Structural mutation detail | RECORDED / INELIGIBLE: 11/11 rows under `data/derived/manifests/`; exposure 9 → artifact 29 |
| Conservative structural exclusions | FROZEN: exclude all six failed source dates; no wall-clock exception |
| Eligible-day calendar and sealed split freeze | PENDING |
| One-day non-research 1s/5m pipeline pilot | PASS for structure/lineage only: explicit 6EH2; DB registered; `research_eligible=false` |
| Research one-second and five-minute derived build | PENDING |
| Event-ordered outcome build | PENDING |
| Numeric fully loaded cost model | PENDING |
| Numeric execution/fill model | PENDING |

Environment readiness does not change any pending row in this table. No
strategy performance result may be produced until the pending input and split
gates are complete. The source-summary, full-QC summary, and pilot exposures,
pilot partitions, and all a-priori proposals are marked
`research_eligible = false`; their PASS or `VALIDATED` states cannot be
promoted into research evidence. The local anomaly-detail manifest adds no
registered exposure by itself.
