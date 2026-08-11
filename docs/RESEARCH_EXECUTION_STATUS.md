# Phase 1 Research Execution Status

- Started: 2026-08-03
- Updated: 2026-08-11
- Campaigns: `phase1_discovery_v1`, `phase1a_conservative_screening_v1`, and
  `bar_pattern_discovery_v1`; the completed state-model lineage is
  `bar_state_conditional_v2` -> `v2a` -> `v2b`
- Campaign mode: `SCREENING_ONLY`
- Maximum positive label: `SCREENING_SURVIVOR`, `research_eligible = false`
- Governing plan: [`RESEARCH_PLAN.md`](RESEARCH_PLAN.md)
- Numeric gates: [`VALIDATION.md`](VALIDATION.md)

## 1. Current Authorization Boundary

The original `phase1_discovery_v1` campaign remains a non-economic control-plane
and pilot record. Phase 1A additionally permits governed conservative screening,
whose maximum positive label is `SCREENING_SURVIVOR`; it cannot produce
`PASS_BACKTEST`, Paper, or Live authority. The governed event-level runner may
execute to produce the cache, checkpoint, and result evidence required by
`VALIDATION.md`; no outcome may be interpreted and no survivor may be declared
until every applicable hard gate passes.

The separate `bar_pattern_discovery_v1` branch screens fixed 5-minute,
30-minute, and 1-hour OHLC hypotheses using derived trade bars and next-bar
entry. It is also screening-only and cannot produce `PASS_BACKTEST`, Paper, or
Live authority. Its exact methodology and completed result are recorded in
[`research/BAR_PATTERN_DISCOVERY_V1.md`](research/BAR_PATTERN_DISCOVERY_V1.md).

The `bar_state_conditional_v2b` engineering successor completed its governed
Discovery run with twelve rejected candidates and no finalist. Its predecessor
failures, exact schema-only amendment, and completed result are recorded in
[`research/BAR_STATE_CONDITIONAL_V2B.md`](research/BAR_STATE_CONDITIONAL_V2B.md).
No walk-forward or holdout access is authorized for this branch.

A-priori proposals belong in `experiments` with no `pattern_id`. Only patterns
actually observed in a registered Discovery exposure belong in
`pattern_ledger`. Every failed or unsupported observation remains recorded.

## 2. Storage Boundary

All row-level derived data is written below the ignored data root:

```text
data/derived/features_1s/...
data/derived/research_5m/...
data/derived/outcomes/...
data/derived/backtest_event_cache/...
data/derived/outcomes/checkpoints/...
data/derived/manifests/...
data/derived/trade_bars/...
data/derived/bar_patterns/...
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
3. Freeze point-in-time active-contract, roll-cutoff, and terminal-exit policy.
   **COMPLETE FOR SCREENING ONLY; definition/status remain required for
   PASS_BACKTEST**
4. Scan every row group for ordering, sequence, reset/snapshot, sentinel,
   mapping, and book-validity quality. **COMPLETE; FAIL**
5. Build the Phase 1A campaign-common source-date proxy calendar. **COMPLETE**
6. Generate and seal the performance-independent split. **COMPLETE**
7. Freeze exact research one-second, five-minute, outcome, cost, and execution
   versions. **COMPLETE FOR SCREENING; SHARED CACHE/REPLAY IMPLEMENTED;
   AUTOMATED TESTS PASS; P5 AND P1_05 REPLAYS COMPLETE**
8. Expose Discovery only in chronological, non-overlapping five-session slices.
   **COMPLETE: 99 OF 99**
9. Register every query, observation, counterexample, variant, and barrier cell.
   **DISCOVERY QUERY/PATTERN STATE COMPLETE; 484-CELL GRID FROZEN; P5 OUTCOMES
   COMPLETE AND SCREENING-REJECTED; P1_05 OUTCOMES COMPLETE AND BOTH DIRECTIONS
   SCREENING-REJECTED**
10. Prove cache integrity, one-pass replay equivalence, complete 484-cell state,
    and exact checkpoint/resume before interpreting outcomes. **P5 RESUMED RUN
    AND INDEPENDENT FULL BYTE-EQUIVALENCE AUDIT COMPLETE; P1_05 REPLAY COMPLETE:
    478 OF 478**
11. Build the neutral trade-bar dataset, preregister 216 fixed OHLC candidates,
    and run Discovery without opening walk-forward or holdout data. **COMPLETE:
    216 OF 216 COMPUTED; 102 SUPPORT-REJECTED; 114 ECONOMIC-REJECTED; NO
    FINALISTS**
12. Train and screen the twelve frozen state-conditional candidates under the
    governed V2 lineage. **COMPLETE UNDER V2B: 12 OF 12 TRAINED; 12 REJECTED;
    ZERO DETERMINISTIC-GATE OR BH-PASSING STATE CELLS; NO FINALISTS**

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

For these four historical non-research exposures, no strategy PnL, barrier
result, or pattern support was calculated. They create no `pattern_ledger` row
and cannot later be presented as independent validation evidence. Later Phase
1A Discovery observations are governed separately in Section 10 and do not
change the authority of these four records.

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

## 8. Historical `phase1_discovery_v1` Control-Plane Snapshot

The following bounded snapshot belongs to the original campaign and pilot. It
must not be read as the later Phase 1A Discovery state:

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

## 9. `PASS_BACKTEST` Blocking Inputs

Phase 1A's conservative assumptions do not resolve the following inputs needed
for `PASS_BACKTEST` or Paper eligibility:

- root-cause classification and governed resolution of the 11 structural
  violations without weakening the frozen gate
- point-in-time `definition` data for verified expiry and instrument terms
- point-in-time `status` data for halts, sessions, and eligible-day classification
- matching MBO book reconstruction for any proposed anomaly reclassification
- independently qualified definition/status calendar and sealed validation split
- actual broker/FCM commission and exchange/regulatory/routing charges
- actual monthly market-data, API, platform, license, account, and operating costs
- measured routing delay, fill, reject, and stop-slippage evidence
- Production-capable roll lead time and terminal-exit execution evidence
- completed shared chronological outcome-engine implementation and validation
- completed walk-forward, stress, and sealed-holdout evidence

`cost_pending_v1` and `execution_pending_v1` preserve the original campaign's
blockers in machine-readable form. Neither configuration may run an economic
screen. Phase 1A instead uses explicitly frozen conservative screening
assumptions, which do not constitute actual cost or execution evidence and do
not satisfy this section.
The missing reference-data contract is recorded in
`configs/data/phase1_reference_inputs_v1.toml`; MBP-10 rows cannot be used to
invent either instrument definitions or trading-status events.

## 10. Completed Phase 1A Discovery State

The governed Phase 1A Discovery prefix covers 495 requested source dates from
2022-01-02 through 2023-08-01. It contains:

```text
five-date AI slices:                 99
successful built source dates:     490
governed no-entry source dates:       5
fixed query exposures:            1,089
accumulated fixed-query patterns:    11
```

All 99 AI result artifacts and their query lineage are immutable and registered.
This completed Discovery state contains source-local feature observations and
fixed-query support, not executable first-touch outcomes. It is the immutable
input to the separately registered p5 outcome result below; Discovery by itself
claims no barrier surface, strategy PnL, screening survivor, walk-forward
result, or sealed-holdout result.

## 11. p5 Completed, Screening-Rejected, and Independently Byte-Verified

The content-addressed date/contract event cache, shared chronological runner,
checkpoint/resume chain, append-only PostgreSQL registry, and operator CLI are
implemented and covered by passing automated tests. Raw MBP-10 may be read by
at most four independent cache-key workers, with at most four partitions in
flight and one cache key per worker. It is never read once per occurrence,
scenario, direction, or barrier cell. After cache publication, one ordered
economic pass updates every registered logical occupancy state:

```text
scenario x direction x contract x 484 cell-occupancy states
```

Late resume and final verification do not load the cumulative detail ledger at
once. They validate, consume, and release one source-date shard at a time, so
record-object memory remains bounded while the full lineage is still checked.
Even a lineage-only path streams each referenced artifact through SHA-256.
Raw/cache Parquet hashing and decoding use the same held descriptor, cache
metadata uses portable `data/`-relative source URIs, and descriptor-relative
no-follow publication rejects pathname or inode replacement.

Source dates advance strictly, and the within-date total order is
`(ts_recv_ns, sequence, event_index, contract_key)`. The cache worker count may
be lowered to one through four, but economic state evaluation is never split by
worker. The configured ceiling and actual runtime value are both recorded.

The runner keeps the 20-active-session first-touch censor clock distinct from
portfolio continuation, checkpoints only at completed-date barriers, and binds
every cache, checkpoint, resumed attempt, and final artifact immutably in
PostgreSQL. Parallelism is limited to independent bounded cache-key builds and
verification. Economic state evaluation remains one logical chronological
pass; it is not parallelized by scenario, direction, contract, occurrence,
time range, or cell.

The completed `p5_01_range_expansion_flow_continuation` replay consumed:

```text
Discovery artifacts:                   99
p5 signals:                          1,111
LONG / SHORT signals:           529 / 582
signal source dates:                    238
futures contracts:                        7
unique replay source dates:              485
date/contract cache partitions:           485
first replay source date:          2022-01-03
final replay source date:          2023-08-31
expected detail rows:                1,613,172
expected aggregate summaries:           2,904
```

The event plan continued after the final Discovery signal on 2023-08-01 so a
position censored at 20 active sessions remained occupied until a real barrier
or mandatory terminal quote. This was a bounded Discovery screen through the
nominal 2023-08-31 pre-expiry boundary, not a full 2022-01-02 through
2026-07-31 backtest. The replay processed 868,723,447 ordered events, emitted
all 1,613,172 detail rows and 2,904 summaries, and completed its 485-date
checkpoint chain. Its canonical final-result SHA-256 is
`ca9f4496c7e7e0102cf40631be060c723c16e16cccf0ef6c78986db35572fd79`.
Each detail row is keyed by `signal_id`, scenario, and one of 484 TP/SL cells
and retains direction and contract. `signal_id` losslessly resolves to the
immutable Discovery occurrence containing every original research variable;
those variables are not redundantly copied into all 1,452 outcome rows for the
same signal. The 2,904 compact summaries are keyed by scenario, direction,
take-profit, and stop-loss and are aggregated across contracts.

The conservative economic screen rejected both p5 directions. This is a
terminal `SCREENING_REJECT` result for p5 under this frozen Phase 1A policy; it
does not provide Backtest, Paper, or Live evidence.

The completed replay used a real checkpoint/resume path. A separate full
uninterrupted replay then reproduced every daily detail shard, checkpoint, cell
summary, and final canonical document byte for byte. PostgreSQL records the
proof as `PASSED` equivalence-audit row `1`, owned by `VALIDATION` RunSpec
`1303` and attempt `1302`, with validation run fingerprint
`b6a227c2f9c768e3b2a32c8bd7a5e2d210e7b3b053d4213b2d01055f6414ab69`.
The 485-checkpoint chain SHA-256 is
`e369bb25566405c46ef5a66268d4d41ba4ed03d6a9ed93fec4443123202946fa`.
The content-addressed proof is stored at
`data/derived/outcomes/audits/phase1a_p5_outcome_equivalence_v1/sha256=b878bdfcd65a481f0710a5be5af5e4c77392260392c164ccd86db1cde6f1d309.json`,
with the same terminal SHA-256. Only this registered proof, not a matching local
file by itself, authorized the second candidate.

## 12. p1_05 Completed and Screening-Rejected

The frozen `p1_05_unconfirmed_move_reversal` input is:

```text
Discovery artifacts:                   99
p1_05 signals:                        943
LONG / SHORT signals:           446 / 497
signal source dates:                    216
futures contracts:                        7
unique replay source dates:              478
date/contract cache partitions:           478
first replay source date:          2022-01-07
final replay source date:          2023-08-31
detail rows:                         1,369,236
aggregate summaries:                    2,904
```

The frozen portable Discovery-artifact manifest SHA-256 is
`23037db1dd12784e379b76effa4f3056cec18d9ae2db7fe7e54e11f2f5424d33`.
The p1_05 signal-manifest SHA-256 is
`733728670870dd438e79dfadd9df80043a0f2baf9553733cf89382132fefba25`, the
cache-request plan SHA-256 is
`3ad39a9bff36e0eae1c87687bf38108b663394624582167bdbf5d848fe5b0252`, the
canonical configuration-parameters SHA-256 is
`d74dff325e2bf4632970f74eeb6515b58960e4c2c090b7663247a8f37f61374a`, and
the checked-in configuration-file SHA-256 is
`c7c756dbe5a7341dc362a08cf8ba71472fd5667bd21200446e9aee5d2470bb99`.

The p1_05 plan-only verification passed. Its 478-partition immutable cache
manifest has SHA-256
`15a35fb14878cae903adaee662c5a8ff27efeea04c373e5f7f4fae02ba03a42c`:
the cache-only preparation reused 474 partitions by exact content identity and
created four, while the final replay reused all 478 verified partitions. Cache
construction by itself contained no p1_05 PnL or barrier conclusion.

The governed replay subsequently succeeded as outcome manifest `4`, RunSpec
`1306`, and attempt `1305`, with run fingerprint
`40730e618651c613be15d303054898757a14f1a9671be6bde7567cc921c7e97e`.
It processed 854,765,427 ordered events, emitted all 1,369,236 detail rows and
2,904 summaries, and completed all 478 source-date checkpoints. The canonical
result is
`data/derived/outcomes/phase1a_p1_05_outcome_replay_v1/sha256=0bd8f465bb3bb47a7f9f72662f905a19a416802a5d8ebff23cdeefd66fcc10ce.json`;
its SHA-256 is the hash embedded in that path. The final checkpoint SHA-256 is
`ede238cf6c45287294cc1dce2927f63dd7d2d8a78dda76f5ff59ec1c102a96de`,
the cell-summary SHA-256 is
`b781d6111bc098fcd846edde3e0a4378ccbefb4edbb34c5e9dae0d5be2dc65be`,
the detail-shard-manifest SHA-256 is
`aca496bacc9606def65c79350a8ca3dbc76f2700d274cdc2badba097fb1fb386`,
and the input-lineage SHA-256 is
`de733b7025eb0c7903fc24679f4adbd8cd859217bf1c68505e1032de75287a00`.
Independent verification rehashed and decoded all 478 detail shards and their
1,369,236 rows, verified all 478 checkpoint files and their chain, matched all
2,904 DB summaries to the result artifact, and reproduced both registered
decisions with the frozen selector. The verified surface is:

| Direction | Baseline positive | Moderate positive | Joint positive | Joint component sizes | Stable cells | Decision |
|---|---:|---:|---:|---|---:|---|
| LONG | 0 / 484 | 85 / 484 | 0 / 484 | `[]` | 0 | `SCREENING_REJECT` |
| SHORT | 5 / 484 | 383 / 484 | 5 / 484 | `[2, 2, 1]` | 0 | `SCREENING_REJECT` |

The five SHORT joint-positive cells have TP/SL distances of 52/96, 56/96,
92/88, 92/96, and 96/88 pips. They form three disconnected components and none
passes the frozen adjacent-stability rule. No cell under any registered
scenario/direction pair has positive calendar-month-loaded net PnL.

Both DB decisions therefore have `positive_region_size = 0`, null selected TP
and SL, and the same rejection reasons:

```text
JOINT_POSITIVE_REGION_NOT_SINGLE_CONTIGUOUS_COMPONENT
NO_INTERIOR_7_OF_9_STABLE_CELL
NO_STABLE_REGION_MEDOID
```

There is no selected entry bracket and therefore no Production Buying Price,
Sell Price, or Loss Price triplet from p1_05. This rejection cannot be promoted
to `PASS_BACKTEST`, Paper, or Live authority.

Every outcome detail resolves through `signal_id` to its immutable Discovery
occurrence, which retains every original research variable. Candidate config,
input manifests, cache reports, terminal resolution, code and dependency
identity, runtime worker count, RunSpec, attempts, checkpoints, daily shards,
summary cells, screening decision, and predecessor-audit lineage are all
recorded. The system does not discard a variable or failed result merely to
reduce the compact outcome artifact.

## 13. Operator Sequence

Apply all migrations through
`0023_bar_pattern_raw_dataset_lineage_fix.sql`, then execute the
modes in order. Every command requires `SYSTEMATIC_FX_DATABASE_URL` or an
explicit `--database-url`.

The completed p1_05 RunSpec used the exact contiguous migration history
`0001`-`0021`. Migration `0021` removed an ambiguous PL/pgSQL record/table alias
from the p1 predecessor lookup without editing applied migration history or
weakening the fail-closed lineage comparisons. The run binds Git object
`d8adc2cd425ac8dda02fe32a2ef4a6571f15f9a5` and exact code-snapshot SHA-256
`be71a0d6664564e6f52391f9ffa45cb610400c51e79edea4d2fb8c962ca0b178`.
Migrations `0022` and `0023` subsequently add the bar-campaign registry and
correct its raw-versus-derived dataset lineage; they do not retroactively alter
the completed p1_05 RunSpec.

```bash
uv run --locked --all-extras systematic-fx db migrate
uv run --locked --all-extras systematic-fx research phase1a-p5-outcomes --plan-only --json
uv run --locked --all-extras systematic-fx research phase1a-p5-outcomes --cache-only --max-cache-workers 4 --json
uv run --locked --all-extras systematic-fx research phase1a-p5-outcomes --max-cache-workers 4 --json
uv run --locked --all-extras systematic-fx research phase1a-p5-equivalence-audit --outcome-replay-manifest-id <p5_manifest_id> --json
uv run --locked --all-extras systematic-fx research phase1a-p1-05-outcomes --plan-only --json
uv run --locked --all-extras systematic-fx research phase1a-p1-05-outcomes --cache-only --max-cache-workers 4 --json
uv run --locked --all-extras systematic-fx research phase1a-p1-05-outcomes --max-cache-workers 4 --json
```

`--plan-only` verifies either the exact p5 1,111-signal/485-partition plan or the
p1_05 943-signal/478-partition plan without writes. `--cache-only` creates or
verifies immutable cache and manifest artifacts below `data/derived` without
reserving an economic attempt. The command without a mode flag starts the
governed replay; issuing the exact same command again verifies the latest
source-date checkpoint and resumes the same active attempt. There is no
separate `--resume` flag, and a terminal failed attempt cannot be reopened.

The audit command accepts `--outcome-replay-manifest-id`, `--database-url`, and
`--json`. The manifest selector is optional only when PostgreSQL has one
unambiguous successful p5 subject. The audit reuses and verifies immutable
caches and never rebuilds them, so it intentionally has no
`--max-cache-workers` option. A zero exit status requires a byte-equivalent
result and successful audit registration.

Cache progress is printed to standard error after the first, every tenth, and
final completed partition. Replay progress is printed after every source-date
checkpoint. The final human-readable or `--json` report is printed to standard
output. Progress, a successful plan audit, or a built cache is not a barrier,
PnL, or survivor result.

Current real-execution state: **P5 COMPLETE AND SCREENING-REJECTED; P5
INDEPENDENT BYTE-EQUIVALENCE AUDIT PASSED; P1_05 REPLAY SUCCEEDED AND BOTH
DIRECTIONS SCREENING-REJECTED**. The completed p1_05 RunSpec binds audit row `1`
and the exact predecessor hashes. This remains a bounded Phase 1A screen through
2023-08-31; it is not `PASS_BACKTEST`, Paper, Live, or a full 2022-2026
validation result, and it provides no Production Buying/Sell/Loss triplet.

## 14. Bar Pattern Discovery V1 Completed Without a Finalist

The governed bar campaign completed on 2026-08-09. It loaded only the 489
Discovery active dates from `2022-01-03` through `2023-08-02`; decisions ended
on `2023-07-10`, before walk-forward fold 1 begins on `2023-08-03`. Every
walk-forward, embargo, holdout, and holdout-tail split remains `SEALED` with no
reveal timestamp.

All 216 candidate attempts succeeded computationally and all 216 trials were
rejected by the frozen screen:

| Timeframe | Support reject | Economic reject | Finalists |
| --- | ---: | ---: | ---: |
| 5 minutes | 2 | 70 | 0 |
| 30 minutes | 28 | 44 | 0 |
| 1 hour | 72 | 0 | 0 |

The result contains 146,864 matched-context evidence rows, 40,906 compact
replay rows, 208 Parquet evidence shards, 216 complete `3 x 484` terminal
surfaces, and one global result. An independent read-only validation rehashed
and schema-checked all 426 live result/evidence files. The global result SHA is
`bda2cfef66c6f59469b77d2d4f85f4ccc531a290934c010f99389262bba8cbfa`;
the evidence-manifest SHA is
`58816efcff5a3051195796b35da3e2c3219892a1da633473218850624a5f6a2e`.

The 5-minute and 30-minute fixed-family candidates are economic rejections
under the conservative cost and grid-stability rules. The one-hour branch is
instead design-limited: normal gap segmentation left no 24-bar signal segment,
so lookbacks 2, 3, 4, 6, and 12 could never assemble ATR20 plus setup history.
It is inconclusive and requires a new versioned context-continuity policy, not
a weakened post-result gate.

There is no finalist and therefore no Buying Price, Sell Price, or Loss Price
triplet. No walk-forward or holdout run is authorized for v1.

## 15. State-Conditional V2B Discovery Completed Without a Finalist

The state-model lineage required two explicitly governed engineering
successors. Frozen V2 stopped during train-only fitting when the 5,000-iteration
SAGA ceiling failed to converge, before predictions or economic evidence were
produced. V2A raised only that computational ceiling to 50,000 under a new
identity; it completed in-memory fitting and replay but failed strict staging of
the first FEATURE Parquet because PyArrow normalized nested-list child names
from `item` to `element`. V2A published no governed research-evidence link.
V2B changed only those two FEATURE schema child names to explicit `element`,
again under a new identity and exact failed-predecessor gate.

V2B then completed the frozen Discovery campaign as campaign `140`, experiment
`7452`. Attempts `1563`-`1574` succeeded and their exact duplicate attempts
`1575`-`1586` were independently reopened and registered as
`SKIPPED_DUPLICATE`. The database contains 144 governed links: 48 FEATURE,
48 LABEL, and 12 each of MODEL, OOS_TRADE, GLOBAL_RESULT, and TERMINAL_RESULT.
All twelve trials are `REJECTED`; the finalist set is empty. The canonical
GLOBAL_RESULT SHA-256 is
`6b377c9f40bd385d6feb174dd8de60be6835772c9dd17f21aac7380ea630c245`.

All twelve inner-fold models trained, and the engine evaluated 349,725 candidate
decision records. Across 588 State cells, zero passed the deterministic and
economic gates and zero passed BH within the frozen 804-hypothesis family. The
best Moderate cell still had fully loaded net EV of approximately `-4.98` ticks
per fill, profit factor `0.847`, and a negative worst-fold EV. This is a broad
economic rejection under the frozen policy, not a finalist lost only to the
multiplicity correction.

The result authorizes no walk-forward, holdout, Paper, or Live continuation.
No selected TP/SL or Production Buying/Sell/Loss price exists. The conclusion
is limited to the exact V2B representation, thresholds, execution rules, costs,
and Discovery interval; it is not a claim that every possible market-state
representation is uninformative.
