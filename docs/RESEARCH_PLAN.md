# Phase 1 Research Plan

- Document version: 1.1.0-draft
- Revised: 2026-08-06
- Status: `DRAFT`
- Parent documents: [`DESIGN.md`](DESIGN.md),
  [`PHASE_1_DESIGN.md`](phases/PHASE_1_DESIGN.md)
- Validation authority: [`VALIDATION.md`](VALIDATION.md)
- Instrument: CME Euro FX Futures (`6E`)

---

## 1. Purpose and Authority

This document bounds what Phase 1 may explore. It owns:

- The AI-directed discovery workflow
- Permitted data representations and feature families
- Hypothesis families and their economic rationale
- Strategy and model families
- Entry, take-profit, and stop-loss search boundaries
- Experiment and variant budgets
- Research priorities and stopping rules

`VALIDATION.md` owns all pass/fail thresholds. This plan cannot weaken those
thresholds or expose the sealed holdout to AI.

The research objective is not to maximize prediction accuracy. It is to find a
reproducible policy that converts point-in-time MBP-10 information into an
executable one-contract bracket with positive fully loaded net economics.

---

## 2. Required Strategy Output

Every candidate must resolve to the following runtime decision:

```text
direction = LONG | SHORT | NO_TRADE
signal_time
entry_order_type
entry_price_or_limit_rule
entry_expiry
take_profit_distance_ticks
stop_trigger_distance_ticks
stop_execution_policy
terminal_roll_and_risk_exit_policy
```

After an entry fill, the runtime must produce exact tick-aligned prices:

```text
LONG:
    take_profit_price = entry_fill_price + take_profit_distance
    stop_trigger_price = entry_fill_price - stop_trigger_distance

SHORT:
    take_profit_price = entry_fill_price - take_profit_distance
    stop_trigger_price = entry_fill_price + stop_trigger_distance
```

A forecast, probability, cluster, anomaly, chart pattern, or feature importance
without this mapping is a research observation, not a strategy candidate.

The stop value is a trigger price. The execution model must separately estimate
the actual stop fill and its slippage.

---

## 3. Market Units and Initial Execution Scope

- Read tick size and contract value from verified instrument definitions and
  store them with each experiment.
- The current 6E research convention is `0.00005` per tick.
- One pip is `0.0001`, or two 6E ticks.
- All calculations, registry parameters, and order prices use integer ticks.
  Pip values are display-only.
- Initial strategy size is one contract.
- Long and short policies are distinct variants even when generated from the
  same hypothesis.
- The initial portfolio permits one open position per strategy and execution
  contract. Signals received while occupied are logged but cannot add or
  reverse exposure.

Contract specifications and fee schedules must be reverified before a new
historical campaign and before Paper or Live use.

---

## 4. Data Layers

### 4.1 Source layer

Use verified MBP-10 for actual-expiry 6E outright contracts. Preserve raw files,
provider metadata, checksums, integer prices, event order, receive/event
timestamps, flags, and book-validity state.

Spreads may inform roll diagnostics but cannot become execution instruments.
Continuous contracts are limited to exploratory visualization and cannot
provide fills, barrier order, or future contract selection.

### 4.2 One-second feature layer

Build point-in-time one-second features from events available before each
second closes. Candidate fields include:

- Midprice, microprice where defensible, and spread
- BBO and cumulative L1/L3/L5/L10 size
- L1/L3/L5/L10 imbalance and depth slope
- Liquidity gaps and distance to material depth
- Trade price, volume, count, and defensible aggressor flow
- Quote and trade intensity
- Return and realized volatility
- Depletion, replenishment, and imbalance-direction changes
- Quote age, stale duration, locked/crossed state, and data validity
- Session, trading status, contract, roll, and time-to-expiry state

### 4.3 Five-minute AI research layer

Five-minute rows are the default AI-visible numeric representation and the
default signal cadence. They must preserve path information rather than only
OHLCV. For appropriate one-second features, calculate:

```text
open | high | low | close
mean | standard_deviation
p05 | p25 | median | p75 | p95
first | last | last_minus_first
minimum_duration | maximum_duration
threshold_persistence
sign_changes | shock_count
valid_seconds | stale_seconds | missing_seconds
```

Do not generate a signal until the five-minute bucket is closed and all
permitted features are ready. Late events cannot rewrite a prior decision row.

### 4.4 Event-level outcome layer

Use event-ordered MBP-10, estimated routing delay, and executable-side book
prices to determine:

```text
ENTRY_FILLED | ENTRY_PARTIAL | ENTRY_EXPIRED | ENTRY_NOT_FILLED
TP_FIRST | STOP_FIRST | TERMINAL_EXIT | CENSORED
```

Five-minute highs and lows cannot determine barrier order. A take-profit touch
is not a fill unless the registered fill model permits it.

Raw MBP-10 files are decoded only while constructing a versioned,
content-addressed date/contract event cache below `data/derived`. Economic
outcomes then consume that cache in one chronological shared pass. Raw sources
must not be reopened per occurrence, stress scenario, direction, or barrier
cell. This storage optimization does not change event order, entry eligibility,
execution semantics, or the requirement to record every cell.

Source dates advance strictly in increasing order. Within each source date,
the frozen cross-contract order is
`(ts_recv_ns, sequence, event_index, contract_key)`. Any duplicate or regression
fails the replay; five-minute row order cannot substitute for this event order.

---

## 5. AI-Directed Discovery Workflow

AI may choose the next bounded computation after inspecting prior Discovery
results. The standard loop is:

```text
1. Load the immutable data dictionary and research constraints.
2. Read one bounded Discovery slice plus the current pattern ledger.
3. Request reproducible statistics, comparisons, or event windows.
4. Record observations, counterexamples, and regime dependencies.
5. Propose or refine an economically motivated hypothesis.
6. Register the experiment and its full search boundary.
7. Run screening only on Discovery data.
8. Reject, retain, or create a registered descendant.
9. Freeze finalists before validation.
```

The default AI slice is five active trading sessions. A larger slice is allowed
only when the active context can retain the data dictionary, constraints,
pattern ledger, and analysis output without truncation.

Five-session slices execute strictly in source-time order. Slice `N > 0` may
not create a run attempt until PostgreSQL verifies that slice `N - 1` has its
exact AI exposure, all frozen query exposures, successful artifact linkage,
and one matching pattern observation per query. Missing, partial, mismatched,
or out-of-order predecessor state is a hard failure.

Do not accumulate all raw slices in conversation history. Persist the exact
numeric state in files and provide AI with compact ledgers plus the current
slice. Automatic or manual context summaries are not authoritative numeric
records.

### All-variable run ledger

Every computation that creates a derived input, exposes numbers to AI, or
evaluates a candidate must be registered before execution with one canonical
`RunSpec`. The specification records, without implicit defaults:

- The campaign and, for candidate-specific work, exact owning experiment
- Every raw, manifest, selection, and derived-input SHA-256
- Calendar and split versions and hashes
- Complete feature, outcome, cost, execution, signal, entry, barrier, and
  terminal policies
- Every threshold, horizon, grid axis, direction, seed, and no-entry rule
- The Git base commit, exact dirty-worktree code/config snapshot, dependency
  lock, and non-secret runtime environment

The canonical JSON SHA-256 is the run fingerprint. Run specifications are
immutable, attempts are append-preserved, and an already successful exact
fingerprint is recorded as `SKIPPED_DUPLICATE` instead of being executed again.
Changing any variable creates a different fingerprint and therefore a distinct
research run. Campaign-common feature/outcome builds use campaign ownership;
strategy and performance runs must name their exact experiment.

Every AI-visible slice or query links directly to the `RunSpec` that produced
it and to an immutable result artifact. The pattern ledger is a compact roll-up;
its slice-level source of truth remains the append-preserved query exposure,
RunSpec, and artifact. Zero-support, no-entry, unresolved, failed, and rejected
results are retained under the same rule as favorable results.

Large per-row or per-occurrence variable records remain in immutable,
content-addressed artifacts below `data/derived`; PostgreSQL stores their exact
SHA-256, URI, producing RunSpec, attempt, and source lineage. This is part of the
run ledger, not an exemption from the all-variable requirement.

Outcome caches, date-boundary checkpoints, checkpoint-resume manifests, and
final surface artifacts follow the same rule. A resumed process continues the
same active attempt and references its original RunSpec, exact cache manifest,
and last verified checkpoint. It may append missing work but may not reopen a
terminal failed attempt, overwrite prior artifacts, reuse a checkpoint under
changed inputs, or relabel a prior result.

If execution stops after an immutable AI artifact is published, a later code
revision must not rebuild or silently relabel that analysis. Recovery first
records a campaign-level `VALIDATION` RunSpec and a content-addressed manifest
containing the complete original FEATURE/AI/QUERY prefix, every query-definition
and query-result hash, the remaining actions, and the new code, dependency, and
runtime identities. Existing analysis remains attributed to its original code;
only missing query projections and pattern registrations are attributed to the
recovery executor. Active, successful, artifact-linked, or mixed feature-only
state without a governed AI exposure is ambiguous and fails closed. A terminal
failed-feature-only state is retryable as a fresh governed execution only when
all matching attempts are `FAILED`, their result/reuse/trade-ledger links are
null, and no AI exposure or pattern observation exists; the failed attempts
remain append-preserved.

### Pattern ledger

At minimum, persist:

```text
observation_id
first_seen_interval
last_updated_interval
feature_definition_versions
direction
entry_condition
economic_rationale
applicable_regime
counterexamples
support_count
candidate_barrier_region
forward_and_first_touch_summaries
cost_assumptions
parent_observation_id
status = OPEN | REGISTERED | REJECTED | PROMOTED
```

Every interval exposed to AI, including summaries and representative examples,
is Discovery data for all affected candidates and descendants.

### Cross-period schedule

The deterministic splitter in `VALIDATION.md`, not AI, owns calendar
boundaries. Discovery must cover its complete assigned history in chronological,
non-overlapping five-session slices. Do not stop after finding a favorable
period.

Before a candidate can become a finalist:

- Run its frozen screening definition across every Discovery slice.
- Publish non-overlapping 60-active-day block results, including losing blocks.
- Apply the same frozen strategy definition to all five walk-forward validation
  folds.
- Keep fold-specific trade, cost, drawdown, barrier, duration, and regime
  results; aggregate performance cannot hide a failed fold.
- Preserve periods with no signals or unresolved barriers in coverage metrics.

A period inspected for failure analysis cannot be reused as independent
evidence for a descendant. New evidence must occur later in time.

---

## 6. Hypothesis Families and Priority

Each registered experiment must identify exactly one primary family. Interaction
terms are allowed, but they inherit the trial lineage of all source families.

### P1. Liquidity-conditioned price continuation and reversal

Question: does a price move continue or reverse depending on spread, depth,
book recovery, and trade participation?

Rationale: identical returns can represent informed pressure, temporary book
depletion, or exhausted flow. MBP-10 may distinguish these states.

### P2. Persistent depth imbalance

Question: do the magnitude, duration, and stability of L1/L3/L5/L10 imbalance
predict which executable barrier fills first?

Rationale: persistence and replenishment may be more informative than a single
imbalance snapshot.

### P3. Aggressor-flow absorption and divergence

Question: does aggressive flow without proportional price response identify
absorption, exhaustion, or delayed continuation?

Rationale: price response conditional on defensible trade flow can separate
liquidity provision from directional pressure.

### P4. Liquidity depletion, replenishment, and gaps

Question: do rapid depth loss, widening gaps, or asymmetric replenishment alter
barrier-first probabilities?

Rationale: short-lived book fragility may precede a move larger than spread and
execution costs.

### P5. Volatility compression and expansion

Question: do low-volatility states followed by rising intensity or changing
depth produce stable breakout or failed-breakout brackets?

Rationale: the expected move and appropriate barrier distance depend on the
current volatility state.

### P6. Session and scheduled-state interactions

Question: are P1-P5 effects conditional on session, weekday, roll state, or
time-to-expiry?

Rationale: liquidity providers, participation, and event risk change through
the trading day and contract lifecycle.

Session effects alone are not accepted without a market-state interaction and
an economic rationale. Scheduled macro-event calendars are outside the initial
MBP-10-only campaign and require a separately versioned data source.

### Approved first outcome sequence

The first shared-replay implementation and screening run is
`p5_01_range_expansion_flow_continuation`. It must complete its cache binding,
chronological replay, 484-cell surface, checkpoint/resume verification, and
immutable lineage audit before `p1_05_unconfirmed_move_reversal` starts. The
second candidate may reuse already verified cache partitions by content hash,
but it receives a distinct RunSpec, attempts, checkpoints, and result
artifacts. These two candidates are not run concurrently, and their order must
not be changed after seeing economic results.

This ordering authorizes implementation and later governed execution only. It
does not state that either candidate's event-level outcome research or economic
screen has completed.

The frozen p5 execution plan consumes all 99 Discovery artifacts and 1,111 p5
signals: 529 `LONG`, 582 `SHORT`, 238 signal dates, and seven futures contracts.
Portfolio continuation extends the event plan beyond the final 2023-08-01
Discovery signal date. It contains 485 unique source dates and 485
date/contract cache partitions, with a nominal cache-request boundary of
2023-08-31 before the final contract's expiry month. The effective terminal is
not assumed to occur on that nominal date: after cache construction, each
contract is reverse-scanned to the latest partition whose report proves a valid
executable quote. This is a bounded Discovery screening run, not the later
full-history walk-forward or sealed holdout test through 2026-07-31.

---

## 7. Permitted Strategy and Model Families

Use the least complex family that can express the hypothesis.

### Priority order

1. Deterministic threshold and state-machine rules
2. Regularized linear or generalized additive models
3. Shallow tree ensembles with bounded depth and feature count
4. Explicitly registered sequence or regime models

Deep neural networks, reinforcement learning, unrestricted genetic search, and
unbounded symbolic search are outside the initial campaign. Adding one requires
a plan amendment, a separate trial budget, and validation capable of measuring
the larger search space.

Every model must emit `LONG`, `SHORT`, or `NO_TRADE` and the registered bracket
policy. A probability threshold is part of the strategy parameters.

### Initial lookback grid

Five-minute lookbacks may use:

```text
3 | 6 | 12 | 24 | 48 | 96 bars
```

These represent 15 minutes through 8 hours. A boundary optimum requires a new
registered experiment before expanding the range.

---

## 8. Barrier Search Plan

### 8.1 Cost floor

Let `C` be the conservative expected round-trip variable cost in ticks,
including any spread or slippage not already embedded in simulated fill prices.

```text
minimum_take_profit_ticks = max(10, ceil(3 * C))
```

Do not double count spread or slippage. A candidate below the cost floor may be
recorded as an economic rejection but cannot advance.

### 8.2 Absolute-distance surface

The Phase 1A registered grid is the complete 12-through-96-pip surface in
four-pip steps:

```text
pips:  12 | 16 | 20 | ... | 88 | 92 | 96
ticks: 24 | 32 | 40 | ... | 176 | 184 | 192
```

Each axis has 22 values. Evaluate the complete 22 by 22 take-profit/stop-loss
Cartesian surface for each fixed signal candidate, subject to the cost floor.
All 484 cells are recorded for multiplicity even when they do not become a
strategy artifact.

If the selected stable region touches 192 ticks, do not extrapolate. Register a
new experiment with a wider bound.

### 8.3 Volatility-normalized surface

Define a point-in-time volatility distance `V_t` from a registered trailing
window. `V_t` must be calculated without future data and rounded to whole ticks.

```text
distance = max(cost_floor, round_to_tick(multiplier * V_t))
multiplier: 0.50 | 0.75 | 1.00 | 1.50 | 2.00 | 3.00 | 4.00
```

Take-profit and stop multipliers may differ. The volatility estimator and both
multiplier ranges must be frozen before execution.

### 8.4 Selection rule

Do not select the single best grid cell. Select a contiguous, economically
positive region under the stability rules in `VALIDATION.md`, then choose its
predeclared representative, normally the region medoid or the least-risk cell
within 10% of the region's median net EV.

---

## 9. Barrier Observation and Position Duration

The strategy has no alpha-imposed maximum holding period. Research still needs
a finite, non-leaking label procedure.

- The default Discovery barrier-observation window is 20 active sessions.
- An entry whose take-profit or stop has not executed inside that window is
  `CENSORED` for first-touch statistics.
- Censored observations cannot be counted as wins, losses, or omitted from
  coverage reporting.
- The deterministic portfolio backtest continues the actual open bracket until
  take-profit, stop, or the mandatory terminal roll/expiry exit.
- Validation splits use the purge and outcome-tail rules in `VALIDATION.md`.
- Duration, capital occupancy, weekend exposure, and skipped signals while a
  position is open are mandatory outputs, not optimization targets.

Changing the 20-session observation window creates a new registered experiment.

### 9.1 Shared chronological evaluation contract

A bounded worker pool may construct independent date/contract cache keys in
parallel. Each exact key is built once, verifies row order and content hashes,
and publishes only under `data/derived`; it is never rebuilt for individual
occurrences or cells. The economic runner waits for the required immutable
cache manifest, then reads its partitions once in source-time order.

Cache request identity uses a portable `data/`-relative raw-source URI. Hashing
and Parquet decoding share one held descriptor for both raw and derived cache
files, while descriptor-relative no-follow traversal binds publication and
reuse to the verified inode. Resume/finalization may skip detail-row decoding
for bounded memory, but never the streaming SHA-256 check.

For the p5 v1 run, `maximum_parallel_workers = 4` and
`maximum_in_flight_partitions = 4`; each worker owns exactly one cache key. The
operator may lower the cache worker count to one, two, or three, but may not
raise it above four. The actual count is recorded in runtime lineage and cannot
create an additional economic replay pass.

For each ordered event the runner updates the complete state product:

```text
registered stress scenario
    x direction
    x execution contract
    x 484 barrier-cell occupancy states
```

The 484 cells share decoded events but not position occupancy. Each cell logs
its own filled entry, skipped signals while occupied, first-touch censor clock,
actual exit, costs, and PnL. The 20-session first-touch state freezes as
`CENSORED` when unresolved at the label boundary; its portfolio state remains
open and continues chronologically until take-profit, stop, or terminal exit.

For terminal resolution, the frozen plan's last pre-expiry calendar partition
is only a candidate. The complete cache reports are grouped by contract and
scanned in reverse; the first partition with a nonzero valid-quote count and
matching last-valid event/time metadata becomes that contract's mandatory
terminal partition. No executable quote before expiry is a hard failure.
Trailing invalid-only partitions are consumed to complete cache verification
but contribute no post-terminal economic events. The versioned policy, full
per-contract result, and semantic SHA-256 are frozen in the RunSpec; the hash is
also in every checkpoint and final-result input lineage, whose cache-manifest
reference binds the underlying report facts.

The futures-contract dimension belongs to chronological occupancy. Output
cardinality is separately frozen: the append-only detail ledger has 1,613,172
signal/scenario/barrier-cell rows (`1,111 x 3 x 484`), while the compact result
has 2,904 scenario/direction/barrier-cell summaries (`3 x 2 x 484`) aggregated
across all seven contracts. Every detail row retains its `signal_id`, direction,
and contract. That `signal_id` losslessly resolves to the immutable Discovery
occurrence where all original research variables remain recorded; the outcome
ledger does not duplicate that variable object 1,452 times per signal.

Checkpoints occur only after a complete source date. Resume must verify the
RunSpec, code snapshot, dependency lock, cache manifest, preceding checkpoint,
and complete scenario/direction/contract/cell state before continuing. An
uninterrupted run and every permitted stop/resume schedule must produce
byte-identical canonical final results.

Prior detail evidence is recovered in daily-shard order and released after it
updates the compact economic accumulators. Resume and final validation are
therefore bounded to one detail shard in memory, not the cumulative
1,613,172-row ledger.

Do not parallelize independent time ranges, signal occurrences, or barrier
cells as separate replays. Scenario, direction, and contract states are also
updated inside the same single logical replay pass, not by independent replay
workers. Pure vectorized arithmetic within one ordered event step is allowed.
A later source date cannot commit before all state for the prior date has
reached the checkpoint barrier.

### 9.2 p5 operator sequence

All checked-in migrations through
`0015_phase1a_outcome_constraints_validated.sql` must be applied before the
governed runner starts. The database URL is required in every mode through
`SYSTEMATIC_FX_DATABASE_URL` or `--database-url`.

```bash
uv run --locked --all-extras systematic-fx db migrate
uv run --locked --all-extras systematic-fx research phase1a-p5-outcomes --plan-only --json
uv run --locked --all-extras systematic-fx research phase1a-p5-outcomes --cache-only --max-cache-workers 4 --json
uv run --locked --all-extras systematic-fx research phase1a-p5-outcomes --max-cache-workers 4 --json
```

`--plan-only` is a read-only verification of the registered 99 artifacts,
1,111 signals, and 485-partition plan. `--cache-only` may publish immutable
cache and manifest artifacts below `data/derived`, but it does not reserve or
start an economic replay attempt. With neither mode flag, the command runs the
single chronological replay. Reissuing that exact full-run command is also the
resume operation: it verifies the RunSpec, cache manifest, and latest
source-date checkpoint and continues the same active attempt. There is no
separate `--resume` mode, and a terminal failed attempt is never reopened.

Progress is written to standard error: cache progress is shown at the first,
every tenth, and final completed partition, and replay progress is shown after
every source-date checkpoint. The compact report or `--json` document is
written to standard output. These progress lines are operational visibility,
not research results or survivor evidence.

---

## 10. Initial Trial Budget

One research campaign is bounded by:

```text
primary hypothesis families:        6
parent hypotheses per family:      10
parent hypotheses total:           60
registered descendants per parent:  3
strategy variants total:          240
sealed-holdout finalists:           10
```

Long and short versions, feature-set changes, threshold changes, model changes,
fixed versus volatility-normalized barriers, and execution-policy changes are
distinct variants.

Barrier-surface cells do not each require a strategy artifact, but every cell
counts in the multiplicity ledger. Unreported exploratory computations are
prohibited.

Reaching a budget does not justify deleting failures or widening it. Close the
campaign, publish its result, and amend this plan before starting another
campaign with a new identifier and fresh validation capacity.

### Pipeline pilot

The existing local seven-file fixture may validate ingestion, feature
construction, AI query flow, barrier ordering, registry behavior, and report
generation. It cannot establish an economic edge or Paper eligibility and does
not consume sealed-holdout capacity.

---

## 11. Required Screening Outputs

Every registered variant reports at least:

- Signal, order, fill, and active-day counts
- Long and short counts separately
- Entry fill and no-fill rates
- Take-profit-first, stop-first, terminal-exit, and censored counts
- Gross and net EV per filled trade
- Fully loaded net PnL
- Profit factor, drawdown, and consecutive losses
- Time-to-hit and total open-duration distributions
- Capital occupancy and skipped-signal counts
- Results by calendar segment, session, volatility state, and contract
- Baseline and stressed spread, fee, latency, and slippage results
- Complete barrier surface and stability-region result
- Trial lineage and multiplicity count
- Representative successes and counterexamples

Accuracy, AUC, raw hit rate, or gross PnL alone cannot promote a candidate.

---

## 12. Research Stopping Rules

Reject or stop a family when any of the following is established:

- Its gross move is structurally smaller than conservative execution costs.
- Its apparent edge depends on unavailable MBO-only or future information.
- Its result disappears under executable-side event ordering.
- Positive economics exist only at an isolated barrier or threshold cell.
- It requires an unsupported queue-position or guaranteed passive-fill model.
- It exhausts its trial budget without a validation-eligible finalist.
- It depends on a single day, contract, event, or unregistered regime.

Stop the overall campaign when every family is rejected, the trial budget is
exhausted, or data quality cannot support independent validation. Do not weaken
`VALIDATION.md` to continue searching.

---

## 13. Campaign Deliverables

- Immutable source-data manifest
- One-second feature specification and version
- Five-minute AI research-table specification and version
- Content-addressed date/contract event-cache specification and manifest
- AI query log and pattern ledger
- Complete experiment and multiplicity registries
- Failed-family archive
- Barrier-surface reports
- Chronological replay checkpoint and exact-resume evidence
- Frozen finalist strategy artifacts
- Validation requests that reveal no sealed-holdout data to AI

---

## 14. Official Contract References

- CME FX product guide:
  https://www.cmegroup.com/markets/fx/fx-product-guide.html
- CME 6E contract-size overview:
  https://www.cmegroup.com/trading/why-futures/welcome-to-cme-fx-futures.html

Record the exact reference and verification date in each campaign manifest.
