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

## M0a deterministic research daemon

M0a is a small, Discovery-only walking skeleton for one finite
`pullback_continuation_v1` search family. It deliberately uses a checked-in,
deterministic 6E MBP-10-like fixture with normal, roll, Friday, and session-close
cases because the production dataset still lacks the verified point-in-time
calendar/status references required for a research-eligible run. It does not
claim alpha and it cannot access sealed holdout data, promote a candidate, or
place an order.

```bash
uv run systematic-fx research m0a build-features
uv run systematic-fx research m0a build-labels
uv run systematic-fx research m0a run-epoch
uv run systematic-fx research m0a daemon start --keep-alive
uv run systematic-fx research m0a report
uv run systematic-fx research m0a verify-invariants
```

The manifest at `epochs/m0a_fixture_v1.toml` freezes 12 real candidates and 24
null/control experiments before execution. Local state is content-addressed
below ignored `.local/m0a/`; generated Markdown goes below
`reports/generated/`. Reruns verify or resume the same bytes and never enlarge
the spent epoch budget.

## M0b bounded real-data bridge

M0b now has a deliberately small real CME 6E bridge, not a full research
epoch. The checked-in CME reference and real-slice manifest stream an exact
four-file allowlist into three four-hour contexts (normal, contract transition,
and Friday close), then publish content-addressed one-second quote/trade,
point-in-time feature, and quote-aware label artifacts. The canonical local
gate produced 33,854 quote seconds, 144 feature rows, and 7,776 labels.

```bash
uv run systematic-fx research m0b materialize-real-slice

SYSTEMATIC_FX_RUN_M0B_REAL_SLICE=1 \
  uv run python -B -m pytest \
  tests/integration/test_m0b_real_slice_materialization.py -q

SYSTEMATIC_FX_RUN_M0B_PG_GATE=1 \
  uv run pytest tests/integration/test_m0b_control_plane_postgres.py \
  tests/integration/test_m0b_holdout_provisioning_postgres.py \
  tests/integration/test_m0b_worker_capability_postgres.py -q -s
```

Every real label is intentionally non-entry-eligible: the bounded CME calendar
proves scheduled hours but not unscheduled trading status, and the September 1
Z2 sample is contract-transition context rather than a previous-day-volume
active-contract selection. Migrations `0029`/`0030` supply the finite
PostgreSQL control plane, immutable CandidateWork binding and a bounded
least-privilege worker cycle; they are tested on disposable databases and are
not applied to the workstation database. No real M0b performance epoch or
deployed autonomous worker service is claimed. The separate holdout
provisioning SQL and verifier live under
`deploy/postgres/` and `scripts/`; the current workstation remains
`NOT_PROVISIONED` until an actual unprivileged daemon login and separate sealed
storage credential pass the denied-read gate. `materialize-real-slice` is
idempotent by content hash; `verify-real-slice --build <build-...json>` reopens
the exact artifact chain without scanning for a substitute.

See [`RESEARCH_ENVIRONMENT.md`](docs/RESEARCH_ENVIRONMENT.md) for setup,
configuration, command, PostgreSQL lifecycle, and recovery details.

## Autonomous AI pattern discovery

Bounded autonomous pattern discovery is implemented and running. A feature-only
symbolic AI examined the visible Discovery prefix—489 active days and 106,605
completed 5-minute decision bars—without labels, outcomes, PnL, walk-forward,
or sealed holdout access. Its current manifest precommits an exact 560-rule
direction-consistent catalog and a 12-proposal output budget, then freezes every
request, compact context, result, and report in an append-only hash chain.

```bash
uv run systematic-fx research ai-pattern run --json                  # Batch 3 once
uv run systematic-fx research ai-pattern verify --json               # Batch 3 replay
uv run systematic-fx research ai-pattern verify --batch 2 --json     # history
uv run systematic-fx research ai-pattern verify --batch 1 --json     # history
```

An independent audit found 60 directionless LONG/SHORT duplicates in the first
620-rule catalog, including one selected rule. Batch 1 remains immutable but is
superseded and cannot advance. Batch 2 rejected those 60 rules before opening
the context, evaluated all 560 corrected rules, and froze 12 hypotheses, but a
final audit found that its pinned commit predated the v2 executable modules.
Batch 3 preserves the same finite catalog and selection policy while binding
the full Python package tree, `pyproject.toml`, and `uv.lock` to committed blob
bytes before context access. Its durable batch SHA-256 is
`dfef5bad188f79af8fa63a6e74f8c9609df34778a9a050278f3740766d24ee4e`.
This is autonomous hypothesis generation, not performance or alpha evidence:
the status remains `HYPOTHESES_GENERATED_AWAITING_ELIGIBLE_DATA`. The current
M0b quote slice has zero entry-eligible labels, so Batch 3 will not be evaluated
or registered as an M0b epoch until official schedule/status/active-contract
evidence makes that transition honest.

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
