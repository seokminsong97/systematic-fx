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
The proposal artifacts remain autonomous hypothesis-generation evidence rather
than alpha evidence. The initial separately precommitted, unsealed local
trade-bar screen evaluated the selected 12 Batch 3 hypotheses under fixed
costs, two outcome-blind null controls, exact daily sign tests, and family-wide
BH correction and selected zero Search finalists.

After those results had already been observed, a separately governed
retrospective expansion completed the other 506 Discovery support-eligible V2
rules. Of the combined 518-rule family, 378 were Search-evaluable and all had
negative fully loaded net ticks; 140 were null/sample-ineligible rather than
economic losers. One 518-member BH correction produced zero rejections, zero
economic-gate passes, and zero finalists. A fresh locked/offline public verifier
recomputed all 43 new mask/result batches and reproduced the immutable family
and report bytes. No later-stage walk-forward, embargo, or holdout 5-minute or
1-second bar payload or row was opened. This retrospective Search expansion is
not a fresh preregistration, strict backtest, sealed holdout, or M0b epoch. The
current M0b quote slice still has zero entry-eligible labels and remains blocked
until official schedule/status/active-contract evidence makes that transition
honest.

Delayed multi-timeframe v1 also completed a separate retrospective 100-member
Search. All 100 candidates were sample-eligible; exactly five had positive net
ticks, but only 12 to 25 fills, and every candidate failed the frozen
sample/economic qualification. BH rejections, economic-gate passes, and
finalists were all zero. Walk-forward and holdout payloads remained unopened,
and a fresh locked/offline public verifier exited `0`. This unsealed screening
result supplies no out-of-sample or alpha evidence.

Retrospective all-cases v1 recovery attempt 5 completed the finite frozen
symbolic/direct/meta design. Search enumerated 37,200 candidate identities and
released four, all symbolic, into five walk-forward folds. The 480 direct/meta
ML candidates were candidate-locally ineligible under the frozen null
construction, with empty fit/result evidence; they were not economic losers.
All four walk-forward candidates were sample-eligible but had negative fully
loaded net ticks; none passed either the family-wide BH decision or the frozen
economic gates. The five-fold stage is this campaign's first out-of-sample
evidence, and it was negative with zero finalists. The terminal lifecycle
therefore recorded `HOLDOUT_SKIPPED` without authorizing or opening holdout
outcomes.
Attempt 5 also corrected all 480 direct/meta public-candidate provenance
bindings that invalidated attempt 4 while preserving its catalogs, costs,
gates, selected strategies, and data lineage. A fresh pinned clean-environment
verifier exited `0` and left the complete artifact tree byte-for-byte
unchanged. This remains retrospective, unsealed local screening over a finite
language—not sealed-holdout, alpha, strict-backtest, Paper, Live, or promotion
evidence.

```bash
uv run --locked --offline python -B -m scripts.run_ai_pattern_holdout verify --json
uv run --locked --offline python -B -m scripts.run_ai_pattern_exhaustive_search verify --json
uv run --locked --offline python -B -m scripts.run_ai_delayed_mtf verify --json
all_cases_root="$(pwd -P)"
/usr/bin/env -i \
  VIRTUAL_ENV="${all_cases_root}/.venv" \
  LC_ALL=C MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  OPENBLAS_NUM_THREADS=1 PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 \
  TZ=UTC VECLIB_MAXIMUM_THREADS=1 __CF_USER_TEXT_ENCODING=0x1F5:0x0:0x0 \
  /Users/seokminsong/.local/share/uv/python/cpython-3.12.13-macos-aarch64-none/bin/python3.12 \
  -s -P -B -S "${all_cases_root}/campaigns/ai_all_cases_v1/bootstrap.py" \
  verify --project-root "${all_cases_root}" --json
```

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
