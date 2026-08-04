# Phase 1 Research Execution Status

- Started: 2026-08-03
- Campaign: `phase1_discovery_v1`
- Campaign state: `DRAFT`, `research_eligible = false`
- Governing plan: [`RESEARCH_PLAN.md`](RESEARCH_PLAN.md)
- Numeric gates: [`VALIDATION.md`](VALIDATION.md)

## 1. Current Authorization Boundary

The campaign may perform data qualification, build non-economic pipeline
pilots, and preregister a-priori parent hypotheses. It may not calculate or
interpret strategy performance until the raw-data, contract/roll, quality,
calendar/split, numeric cost, and numeric execution gates are complete.

A-priori proposals belong in `experiments` with no `pattern_id`. Only patterns
actually observed in a registered Discovery exposure belong in
`pattern_ledger`. Every failed or unsupported observation remains recorded.

## 2. Storage Boundary

All row-level derived data is written below the ignored data root:

```text
data/derived/features_1s/...
data/derived/research_5m/...
data/derived/outcomes/...
data/derived/manifests/...
```

PostgreSQL stores only identities, states, compact summaries, checksums, URIs,
and source lineage. Content-addressed derived Parquet snapshots and their
lineage manifests also remain under `data/derived/registry/` and
`data/derived/manifests/`. The separate `artifacts/` tree may hold compact
control-plane registration documents, AI context packages, and reports, but it
must not hold row-level derived market data.

## 3. Gate Order

The implementation order is fixed:

1. Compute every raw file's full-content SHA-256. **COMPLETE**
2. Register the source catalog and immutable hashes. **COMPLETE**
3. Verify actual-expiry metadata and freeze a contemporaneous active-contract,
   roll-cutoff, and terminal-exit policy.
4. Scan every row group for ordering, sequence, reset/snapshot, sentinel,
   mapping, and book-validity quality. **COMPLETE; FAIL**
5. Build the campaign-common eligible calendar.
6. Generate and seal the performance-independent split.
7. Freeze exact research one-second, five-minute, outcome, cost, and execution
   versions. The bounded `mbp10_pilot_v1` contract does not satisfy this gate.
8. Expose Discovery only in chronological, non-overlapping five-session slices.
9. Register every query, observation, counterexample, variant, and barrier cell.

## 4. Verified External Contracts

Verified on 2026-08-03:

- Databento MBP-10 contains every trade and aggregate book update across the top
  ten price levels. Trading-status events are a separate schema and must not be
  fabricated from MBP-10.
- For `GLBX.MDP3`, trade side is the aggressing side when the source message
  supports that inference; side `N` remains unclassified and is reported
  separately.
- CME 6E is 125,000 EUR. The Globex outright minimum tick is `0.00005`, worth
  USD `6.25` per contract.
- CME Rulebook Chapter 261 terminates trading on the second Business Day before
  the third Wednesday of the contract month, with additional banking-holiday
  provisions. A conservative project roll rule still must be frozen separately.

References:

- <https://databento.com/docs/schemas-and-data-formats>
- <https://databento.com/docs/venues-and-datasets>
- <https://www.cmegroup.com/markets/fx/fx-product-guide.html>
- <https://www.cmegroup.com/content/dam/cmegroup/rulebook/CME/III/250/261/261.pdf>

## 5. Recorded Non-Research Exposures

Four immutable, AI-visible exposure rows are registered, all with
`research_eligible = false`:

- A `SUMMARY` exposure covers source qualification for 2022-01-02 through
  2026-07-31. It records footer/hash manifest identities and compact catalog
  counts only; no performance field was requested.
- A `PIPELINE_PILOT` exposure covers the explicit 2022-01-03 `6EH2` / provider
  instrument `28727` build. Its allowed use is pipeline mechanics and
  structural qualification only.
- A `SUMMARY` exposure covers the completed full structural QC result. It
  records complete coverage, the six failing files, and the failed research
  gate without exposing strategy performance.
- An `EVENT_WINDOW` exposure records the exact 11 mutation pairs and the
  conservative six-source-date exclusion. Its result artifact is the
  deterministic JSONL under `data/derived/`; no performance field was requested.

The 2022-01-03 file has been used only as a non-research pipeline pilot. The
visible checks include one row-group structural smoke and whole-file aggregate
counts used to identify the dominant outright for builder development. This
interval is not independent validation evidence for this campaign or any
descendant that uses those observations.

No strategy PnL, barrier result, or pattern support has been calculated. These
exposures create no `pattern_ledger` row and cannot later be presented as
independent validation evidence.

The explicit `6EH2` / provider instrument `28727` pilot produced 48,540
observed-second rows and 281 five-minute rows from 1,158,550 selected events.
The source checksum
`f682bae3b618f8905a76be1ff144a49620aea510ae3828ef245ffc0a53c7c2f8`
matched the full-content manifest. Only one five-minute window met the
deliberately strict pilot requirement of 300 observed event seconds; 280
windows merely had a complete source-date interval. Therefore this pilot proves
pipeline mechanics, not the final research feature contract. The research
version still needs reference trading status, a reset-aware point-in-time time
grid, and a frozen staleness policy.

## 6. Completed Source Hash Gate

All 1,434 source files and 156,675,982,394 bytes were streamed through SHA-256.
The canonical manifest is
`data/derived/manifests/mbp10_source_sha256_v1.jsonl`, whose SHA-256 is
`14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de`.
An immediate checkpoint-only rerun reproduced it with 1,434 resumed files and
zero rehashed files.

The paired footer and full-content manifests are now atomically registered as
dataset `glbx_mdp3_mbp_10_6e_fut_v1`. PostgreSQL contains 1,434 `source_files`,
all in `HASHED` state with their individual SHA-256 values. The dataset remains
`VALIDATING`; source registration alone does not promote it to `READY`.

The canonical bounded qualification evidence is
`data/derived/manifests/mbp10_source_qualification_v1.json`, SHA-256
`8f0c7c090c768a48e8cd7e788816689ee503082f8c53169340f813777632ebf9`.
Its PostgreSQL artifact and eight dataset-target `quality_checks` preserve the
then-current bounded results rather than only passes:

```text
PASS: footer exact contract/identity
PASS: full-content hash and database identity 1:1
PASS: mapping classification (unknown = 0)
WARN: provider partial metadata (6,342 symbols)
FAIL: point-in-time instrument definitions absent
FAIL: point-in-time trading status absent
FAIL: eligible-day calendar absent
FAIL: every-row-group quality scan pending
```

The last line is the immutable state at bounded-report creation time. The later
full scan closed “pending” with a structural `FAIL`, not a pass; the historical
row is preserved rather than rewritten.

An immediate replay created zero new files, artifacts, or checks. Neither run
changed the dataset from `VALIDATING` or any source from `HASHED`.

## 7. Completed Full Structural QC: Failed Gate

The frozen `mbp10_structural_qc_v1` scanner completed 1:1 coverage of all 1,434
files, 49,846 row groups, and 3,220,073,651 rows. The immutable file-level
manifest remains under `data/derived/manifests/` with SHA-256
`47efca0afcd8ac97b78f3a5b7ce6b1194ef83d9764fa30314e253cbcbd296ffc`.
It reports 1,428 `PASS` files and six `FAIL` files. All 11 hard violations are
`clean_trade_none_book_mutation`:

```text
2024-06-30: 2
2024-07-01: 2
2024-07-14: 2
2026-04-19: 3
2026-06-07: 1
2026-06-21: 1
```

That counter identifies a clean `T` or `N` row whose ten-level book differs
from the preceding comparable physical row for the same instrument. The frozen
gate has zero tolerance, so complete coverage does not imply a pass. The
structural result is `FAIL`, and the dataset remains research-ineligible.
Clock/sequence anomalies, snapshot/bad-book flags, and incomplete or
locked/crossed BBO observations remain separate non-gating diagnostics for
later status/session-aware classification.

The completed PostgreSQL registration created 1,434 source checks, dataset
structural check `1451`, and diagnostic check `1452`. Aggregate structural
status is `FAIL`; diagnostics are `WARN`. Artifact `27` and job `22` point to
immutable evidence under `data/derived/manifests/full_qc_registry_v1/sha256/`
with SHA-256
`adb894b20cf0e60819a31e18f9c64c74975ced18916eb4aefd9112ae4dc9c355`.
Dataset `5` remains `VALIDATING`, all 1,434 sources remain `HASHED`,
`status_effect=NONE`, and `research_eligible=false`. An immediate replay
created nothing, confirming append-only idempotence.

### Detailed mutation evidence and conservative exclusion

A separate performance-free local manifest expands the 11 hard counters into
exact previous/current same-instrument row evidence:

```text
path:   data/derived/manifests/mbp10_clean_trade_none_book_mutations_v1.jsonl
SHA-256: 8f77077ae9243a2eedb893455a49bad1f75ff6ea02629a4ca442596e6ec67d23
lines:  11
bytes:  39,567
```

Every pair has previous and current `action=T`, `side=N`, and `flags=0`, with
exact `ts_event` at `22:00:00Z`. Ten books move from crossed to normal and one
from locked to normal; each transition changes between 5 and 56 of the 60
ten-level book fields. The common wall-clock boundary is evidence to
investigate, not an allowed exception. Under the frozen MBP-10 interpretation,
the rows carry no snapshot, reset, or bad-book marker that would reclassify the
book replacement, so all 11 remain hard violations.

`configs/data/phase1_structural_exclusions_v1.toml` therefore freezes the
conservative interim action: exclude each of the six entire source dates from
the campaign-common eligible calendar. It prohibits a hard-coded `22:00Z`
exception and implicit reclassification. Reconsideration requires point-in-time
`status` evidence and matching book reconstruction, using MBO when needed,
under a new checker version.

This exclusion policy is not a repair and does not convert the source catalog
to `PASS`. The full dataset remains structurally `FAIL`, `VALIDATING`, and
research-ineligible. `data inspect-qc-mutations` replays the six failed sources
and requires per-file manifest, unmodified scanner-helper, and independent
detail counts to agree; its immediate replay reproduced identical bytes.
PostgreSQL exposure `9` (`EVENT_WINDOW`) links the JSONL as artifact `29`, with
the same SHA-256 and `research_eligible=false`. Re-registering it created no
new exposure or artifact.

## 8. Governed Control-Plane State

The current PostgreSQL state is intentionally bounded:

- Campaign `phase1_discovery_v1` is `DRAFT`, with a 240-strategy-variant budget
  and at most 10 sealed-holdout finalists.
- Exactly 60 a-priori parent experiments are `REGISTERED`: 10 each in families
  `P1` through `P6`. All 60 have `pattern_id = NULL`.
- `pattern_ledger = 0`, `experiment_trials = 0`, and no backtest performance has
  been created. A registration artifact and a succeeded registration job
  preserve the 60 proposals and the pending cost/execution assumptions.
- The 48,540-row `FEATURES_1S` partition and 281-row `RESEARCH_5M` partition are
  recorded as `VALIDATED` with a succeeded build job, a canonical lineage
  manifest, and exact linkage to the 2022-01-03 source SHA-256. Here
  `VALIDATED` means **structure and lineage only**; both partition metadata and
  the build record say `research_eligible = false`.
- The pilot registry keeps content-addressed Parquet snapshots below
  `data/derived/registry/pilot_v1/` and its canonical manifest below
  `data/derived/manifests/pilot_derived_registry_v1/`. No pilot row data is
  placed in PostgreSQL or the top-level `artifacts/` tree.
- Because point-in-time provider definitions are not registered yet, the pilot
  partitions keep `instrument_id` foreign keys null and retain provider ID
  `28727` and raw symbol `6EH2` in lineage metadata instead.
- Integration tests now use a separately bootstrapped `systematic_fx_test`
  database. The research database was re-audited after the test run, and the
  test database retained only its two migration rows and no test-owned data.
  The final suite passed 148 tests and 27 subtests; the database-required doctor
  reported zero failures and zero warnings.

The canonical pilot lineage manifest SHA-256 is
`b93c9551f279b3e717fc9920780ee4b9316ca17ae30ff1576b4fc4511bdf1245`.

## 9. Blocking Inputs

The following values remain intentionally unresolved:

- root-cause classification and governed resolution of the 11 structural
  violations without weakening the frozen gate
- point-in-time `definition` data for verified expiry and instrument terms
- point-in-time `status` data for halts, sessions, and eligible-day classification
- matching MBO book reconstruction for any proposed anomaly reclassification
- campaign-common eligible calendar and performance-independent sealed split
- broker/FCM commission and exchange/regulatory/routing charges
- monthly market-data, API, platform, license, account, and operating costs
- baseline routing delay and entry order/expiry policy
- partial/passive fill and stop-order policy
- stop slippage and same-timestamp event tie-break policy
- exact roll lead time and terminal-exit execution rule

`cost_pending_v1` and `execution_pending_v1` preserve these blockers in
machine-readable form. Neither configuration may run an economic screen.
The missing reference-data contract is recorded in
`configs/data/phase1_reference_inputs_v1.toml`; MBP-10 rows cannot be used to
invent either instrument definitions or trading-status events.

## 10. Current Transition Boundary

The database campaign status remains `DRAFT`. No eligible-day calendar,
campaign split, research outcome partition, experiment trial, strategy, or
backtest result has been registered. The next work is structural-failure
resolution plus reference-data qualification, followed by an eligible calendar
and performance-independent split freeze. The existing source summaries, pilot
outputs, and a-priori proposals do not permit skipping those gates. Numeric
cost and execution configurations also remain pending.

Campaign-common eligible days are determined without a strategy result.
Candidate-specific active-entry days are evidence counts evaluated later and
cannot move the common split boundaries.
