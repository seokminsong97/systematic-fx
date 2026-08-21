# Phase 1 Design: Deterministic Research and Backtesting

- Document version: 1.12.0-draft
- Revised: 2026-08-20
- Status: `DRAFT`
- Parent document: [`DESIGN.md`](../DESIGN.md)
- Input: Historical MBP-10 from the Data Source
- Governing documents: [`VALIDATION.md`](../VALIDATION.md),
  [`RESEARCH_PLAN.md`](../RESEARCH_PLAN.md)

---

## 1. Objective

Phase 1 runs reproducible, finite-budget Discovery computations and turns
measured patterns into registered executable strategy artifacts. Standard
artifacts use executable brackets. Only an artifact expressly named by
`DESIGN.md` may use a fixed-horizon exit, and that permission is limited to
governed historical and no-order shadow research. An optional LLM may propose a
hypothesis before an epoch is frozen, but it is never part of the daemon runtime
loop or a result/promotion authority.

```text
Verified MBP-10
    ↓
one-second features and five-minute research rows
    ↓
finite precommitted search epoch
    ↓
Registered experiment
    ↓
directional entry and immutable registered exit policy
    ↓
Deterministic backtest
    ↓
Validation and stress tests
    ↓
Reject or REGISTER for separate evaluation
```

## 2. Document Authority

### `RESEARCH_PLAN.md`

Owns:

- Hypothesis families and economic rationale
- Permitted feature scope
- Model and rule families
- Trial budgets and parameter boundaries
- Research priorities

### `VALIDATION.md`

Owns:

- Train, validation, and sealed-holdout splits
- Walk-forward folds and leakage controls
- Minimum trade and active-day requirements
- EV, uncertainty, and drawdown thresholds
- Multiplicity correction
- Paper eligibility
- Minimum evidence for Live approval

### This document

Owns:

- Research pipeline
- Strategy artifacts
- Backtest semantics
- Cost and execution-model boundaries
- Experiment registry
- Limits on AI authority

---

## 3. Research Scope

### Instrument

- Actual-expiry CME 6E contracts
- One active execution contract at a time
- Continuous series only for discovery and visualization support

### Decision intervals

- Event-level MBP-10 remains the source for book state, ordering, first-touch,
  and simulated execution.
- One-second buckets are the default layer for intrabucket feature generation.
- Five-minute closed buckets are the default discovery rows and signal
  decision interval.
- A faster signal interval requires a separately registered research campaign
  and evidence that its economic value justifies the added data and execution
  requirements.

### Exit horizon

- The primary strategy exit is the first executed take-profit or stop-loss OCO
  child.
- Phase 1 imposes no alpha-driven maximum holding period.
- Time to take-profit, time to stop, total open duration, capital occupancy,
  weekend exposure, and unresolved observations are mandatory metrics.
- Risk, emergency, roll, and delivery-avoidance exits override the strategy.
- Discovery labels that do not resolve inside the registered observation window
  are censored, never silently dropped or counted as wins or losses.
- M0a is a bounded engineering exception that explicitly preregisters a
  volatility-normalized 30/60/120-minute label horizon. It is not a deployment
  holding policy and cannot create Paper eligibility.

### Governed e2a single-candidate replay

The following semantics apply only to `e2a_month_end_v1` and are not a general
candidate generator:

- Instrument: CME 6E selected by the `trade_bar_v1` previous-session-volume
  active-contract rule.
- Event day: group structurally eligible active dates by calendar
  `(year, month)`, retain dates whose weekday is Monday through Friday, and take
  the maximum date in each group. Metas-adjacency or comparison with the next
  manifest row is prohibited.
- The calendar implementation must retain the regression dates `2022-07-29`,
  `2023-04-28`, `2025-08-29`, and `2025-11-28`.
- `month_open_px`: the last one-second trade close at or before 60 seconds after
  the first bar of the month's first eligible day begins, with a 3,600-second
  staleness cap.
- `p15`: the last trade price at or before 15:00:00 `Europe/London`, with a
  1,800-second staleness cap. London daylight-saving conversion is part of the
  frozen rule.
- Direction: `-sign(p15 - month_open_px)`. Missing input or equality emits
  `NO_TRADE`.
- Entry: decide at 15:00:00 London and use the prevailing best ask for a long or
  best bid for a short at decision plus one second, with a three-second wait
  cap.
- Exit: target entry fill time plus 86,400 seconds; take the first valid
  opposite-side BBO at or after the target, including across weekends.
- Force exit at active-contract change or a stream gap greater than 96 hours
  using the frozen campaign's deterministic last-valid-quote rule.
- One position is allowed. There is no pyramiding, take-profit, stop, or target.

Calendar, clock, event, and month-open cross-reference values are first-class
artifact fields, not post-processing filters. The 24-hour fixed horizon is a
legitimate replay exit for this named research path even though it exceeds the
existing six-hour catalog limit. The artifact must remain isolated from the
frozen Phase 1A, M0b CandidateWork, bar-pattern, delayed-MTF, and all-cases
catalogs; none of their hashes, search spaces, or closure claims may change.

All historical data exposed for this family are now in-sample. Historical
replay may verify the exact artifact but must not tune any value or claim a new
sealed holdout. This subsection does not create broker Paper eligibility.

---

## 4. Feature Scope

### Production-compatible candidates

- Midprice and spread
- BBO and cumulative L1/L3/L5/L10 depth
- Depth imbalance and slope
- Liquidity gaps
- Trade volume, count, and defensible aggressor flow
- Short-horizon return and realized volatility
- Quote age and stale state
- Session, roll, and trading status

### Multiresolution representation

The five-minute research table must not reduce MBP-10 to OHLCV alone. Build
one-second point-in-time features first, then preserve economically meaningful
five-minute distributions and paths, including where available:

- Open, high, low, close, mean, standard deviation, and quantiles
- Last value and change from bucket open
- Extreme duration and threshold-persistence time
- Direction changes and depletion/replenishment counts
- Trade and quote intensity
- Valid, stale, missing, and locked/crossed-book durations

All aggregates close before the signal decision. A feature may not be revised
with late or future information after its decision bucket closes.

### Excluded from the current input

Phase 1 consumes MBP-10. It does not consume:

- Individual order add/modify/cancel events
- Exact order lifetimes
- Queue position or exact priority
- Same-order replacement paths
- Order-level concentration

Using MBO-only features requires a separate data contract and an amendment to
the high-level design.

### Platform-specific features

Features available only from IBKR or Rithmic must be tested as separate
strategy families after platform evaluation. They must not be mixed into the
common champion beforehand.

---

## 5. Deterministic Research Loop and Optional Proposer

### An optional external proposer may inspect and propose

- Discovery-only feature rows, registered summaries, and representative event
  windows
- Reproducible queries, groupings, comparisons, and visualizations
- Hypotheses
- Feature transformations
- Regime definitions
- Entry and exit rules
- Absolute and volatility-normalized take-profit and stop-loss distances
- Model families
- Bounded hyperparameter ranges
- Failure analyses and next experiments

These proposals must be converted to an immutable epoch manifest before the
daemon starts. The daemon continues normally when the proposer is absent,
fails, or times out; the proposer cannot choose the next runtime experiment,
approve a result, continue a spent epoch, or alter a budget.

### AI and the daemon may not

- Modify `VALIDATION.md`
- Reuse the sealed holdout
- Delete trial counts or failed results
- Remove trading costs
- Bypass experiment registration or validation
- Inspect sealed-holdout features, labels, trades, or aggregate results before
  the artifact is frozen
- Treat AI-visible chronological slices as independent evidence
- Open sealed holdout data, promote to Paper/Live, or place an order

Live authority and capital restrictions are governed by `DESIGN.md`.

### Experiment registration

Freeze at least the following before execution:

```text
experiment_id
hypothesis_family
hypothesis
feature_set
signal_rule_or_model
parameter_range
entry_and_exit_policy
direction_policy
entry_order_and_price_policy
take_profit_policy
stop_trigger_and_execution_policy
barrier_observation_window
terminal_exit_policy
cost_model
execution_model
train_validation_plan
random_seed, when applicable
parent_experiment_id
dataset/feature/label/execution/code versions and hashes
real candidate budget and null candidate budget
admission rules and parent epoch
```

Changing a parameter boundary or success criterion after execution creates a
new experiment.

Every period exposed to AI or used to select, reject, or refine a candidate is
Discovery data for that candidate and its descendants. Period summaries do not
recover out-of-sample status.

---

## 6. Strategy Artifact

A strategy is more than a model file. Its artifact contains:

```text
strategy_id
strategy_version
hypothesis
feature_set_version
signal and entry rules
direction and entry order policy
exit_family = bracket | governed_fixed_horizon
take-profit, stop-trigger, stop execution, and OCO rules when exit_family = bracket
calendar, clock, cross-reference, horizon, and force-exit rules when
exit_family = governed_fixed_horizon
position sizing rule
applicable regimes
contract and roll policy
training and validation intervals
cost and execution model versions
source data checksums
code commit and container digest
backtest results
validation decision
```

`governed_fixed_horizon` is valid only for an artifact expressly named by
`DESIGN.md`; it is not a generic registry option and carries no Paper or Live
authority.

An artifact change does not inherit prior evidence automatically.

---

## 7. Backtest Model

### Information availability

Each decision may use only information available at that time.

- No future contract selection
- No future volume or open interest
- No retroactive insertion of late events into closed buckets
- No label-period information in features
- No assumption of zero provider delay

### Decision timeline

```text
market events
    ↓
bucket close
    ↓
features ready
    ↓
signal decision
    ↓
risk decision
    ↓
routing delay
    ↓
estimated exchange arrival
    ↓
fill / partial fill / no fill
    ↓
broker-managed OCO protection
    ↓
take-profit first | stop first | terminal risk exit
```

### Execution assumptions

Prohibited:

- Mid-to-mid fills only
- Zero-latency fills at the decision quote
- Immediate passive fills on touch
- Excluding spread, fees, or slippage
- Infinite liquidity
- Guaranteed fills at stop-trigger prices

Permitted initial models:

- Market orders consume opposite-side depth after estimated arrival.
- Marketable limits fill only within the limit.
- Passive limits use a conservative fill or no-fill model.
- Stops include routing delay and slippage after trigger.
- Every simulated entry applies its immutable registered exit family. Standard
  candidates apply take-profit, stop-loss, OCO, and terminal risk/roll rules.
  `e2a_month_end_v1` applies only the governed fixed-horizon semantics above.
- Barrier results are determined from executable-side MBP-10 event order, not
  from five-minute high/low ordering.
- A take-profit touch is not a fill unless the registered execution model
  permits the fill. A stop price is a trigger, not a guaranteed fill.
- Only one open position per strategy and contract is permitted in the initial
  research scope; signals while occupied are recorded but do not create
  overlapping exposure.

An exact queue model is not accepted as Production evidence under the current
scope.

### Shared chronological replay architecture

Raw MBP-10 is decoded into an immutable event cache before economic replay.
Each cache partition has the immutable key `(source_date, raw_symbol)`, is
content addressed by its canonical bytes, and is stored only below
`data/derived`, for example:

```text
data/derived/backtest_event_cache/
    <cache_version>/
    sha256=<content_sha256>.parquet
```

Its manifest binds each partition to the raw source SHA-256, selected contract,
schema and decoder versions, row count, event-order bounds, and content SHA-256.
A bounded pool may process independent date/contract cache keys in parallel.
Each exact cache key is built once; workers may not rescan raw data for
individual occurrences, scenarios, directions, or grid cells. Publication is
atomic, and an existing path is reused only after byte size and SHA-256 verify
exactly.

Cache identity is portable across workspace moves: request indexes and Parquet
metadata retain a `data/`-relative source URI rather than an absolute path.
The raw source and resulting cache are each hashed and read through the same
held file descriptor. Directory traversal and hard-link publication use
descriptor-relative no-follow operations and verify the final pathname inode,
so path, symlink, or same-size byte replacement cannot be certified under the
original hash.

The Phase 1A p5 and p1_05 v1 cache builders have a governed ceiling of four
worker processes and four in-flight partitions. One worker owns one cache key
at a time. The operator may lower the worker count to any value from one
through four, and the actual value is recorded in the RunSpec runtime identity.
A content-addressed semantic request index maps the exact
source/date/contract request to its already verified cache artifact so an
ordinary exact rerun does not reopen raw MBP-10.

Economic replay opens the verified cache partitions in strict source-time
order once. The shared event stream fans out to the complete registered state
space:

```text
stress scenario
    x LONG | SHORT
    x execution contract
    x 484 take-profit/stop-loss cells
```

Every cell owns its own position-occupancy state because its exit time and
therefore its skipped signals may differ. Threshold calculations may be shared
or vectorized, but no cell may be pruned and cells may not be evaluated by
independent raw-file replays. An event is applied in canonical order before the
next event or source date is admitted.

Source dates are processed in strictly increasing order. The canonical merge
key within a source date is
`(ts_recv_ns, sequence, event_index, contract_key)`. It is a strict total order;
a duplicate or regression is a hard replay failure.

The contract dimension above is portfolio occupancy state, not the identity of
the final aggregate summary. The completed frozen p5 result ledger contains
1,613,172 detail rows (`1,111 x 3 x 484`), and the completed p1_05 result ledger
contains 1,369,236 (`943 x 3 x 484`). Each row represents one signal, scenario,
and barrier cell, with the signal's direction and futures contract retained. Each
candidate's final compact surface contains 2,904 summaries (`3 x 2 x 484`),
aggregated across the seven futures contracts and keyed by scenario, direction,
take-profit ticks, and stop-loss ticks. The row's `signal_id` resolves to the
immutable Discovery occurrence that preserves every original research
variable.

The first-touch label clock and portfolio clock are separate state variables.
If neither registered barrier executes inside 20 active sessions, the
first-touch observation is permanently `CENSORED`. The corresponding portfolio
position nevertheless stays occupied, continues to block new signals for that
cell, and consumes later events until actual take-profit, stop, or mandatory
terminal roll/expiry exit. A later portfolio exit must not rewrite the censored
first-touch label.

The nominal final pre-expiry cache date is not terminal authority. Once every
required cache report is available, each contract is scanned in reverse to the
latest partition with a valid executable quote and coherent last-valid
event/time metadata. That versioned policy and full per-contract selection are
hashed into the RunSpec, and the semantic hash is repeated in checkpoint and
final-result input lineage. The cache-manifest identity binds the report facts
used by the decision. Later invalid-only partitions remain verification inputs
but are not post-terminal economic events; a contract with no pre-expiry
executable quote fails closed.

### Checkpoint, resume, and parallel boundaries

Checkpoints are allowed only at deterministic completed-source-date barriers.
Each content-addressed checkpoint records the last consumed cache partition,
signal cursor, first-touch clocks, open bracket and occupancy state for every
scenario/direction/contract/cell, accumulated counters, prior checkpoint hash,
and the exact RunSpec and cache-manifest hashes. PostgreSQL stores the immutable
artifact identity and attempt lineage. Resume verifies all bindings before
reading the next date and continues only the same active attempt. Attempts and
checkpoint rows remain append-preserved; a terminal failed attempt is never
silently reopened. Every permitted resume must produce the same final artifact
bytes as an uninterrupted run.

Resume rebuilds compact economics by hash/schema-validating and consuming each
prior daily detail shard in order, then releasing that shard before opening the
next. Final artifact verification follows the same bounded-memory rule. The
implementation must not retain either complete candidate detail ledger as
Python objects. A lineage-only load may omit Parquet decoding, but it must still
stream and compare the complete artifact SHA-256.

Candidate order is a database-enforced research boundary. The completed p5
resumed replay and its screening rejection were not enough by themselves to
start p1_05. A separate full uninterrupted replay had to reproduce every p5
daily shard, checkpoint, summary, and final-result byte, and its
content-addressed proof had to be registered under a successful `VALIDATION`
attempt and a `PASSED` append-only equivalence-audit row. The p1_05 RunSpec and
downstream lineage must bind that audit identity plus the exact predecessor
replay-manifest, run-fingerprint, result, input-lineage, cell-summary,
detail-shard-manifest, and final-checkpoint hashes. Planning and cache
construction do not reserve an economic attempt and may occur before this gate
passes.

The implemented boundary passed exactly as designed. Equivalence-audit row `1`,
owned by `VALIDATION` RunSpec `1303` and attempt `1302`, byte-verified all 485 p5
checkpoints. It authorized p1_05 RunSpec `1306`, whose governed replay completed
478 checkpoints, 854,765,427 ordered events, 1,369,236 detail rows, and 2,904
summaries under manifest `4` and attempt `1305`. Its result SHA-256 is
`0bd8f465bb3bb47a7f9f72662f905a19a416802a5d8ebff23cdeefd66fcc10ce`.
Independent verification reproduced the DB selector decisions: both LONG and
SHORT are `SCREENING_REJECT`, with no stable cell, null selected TP/SL, and
`positive_region_size = 0`. No scenario/direction has a calendar-month-loaded
positive cell. Therefore the implementation produces no Production
Buying/Sell/Loss triplet. This is screening evidence only; it does not satisfy
the walk-forward, sealed-holdout, or `PASS_BACKTEST` requirements.

The next bounded economic screen is the already-registered P4 liquidity pair,
not a threshold revision of either rejected candidate. Its immutable protocol
is recorded in
[`PHASE1A_P4_PAIR_OUTCOME.md`](../research/PHASE1A_P4_PAIR_OUTCOME.md).
P4-01 and P4-02 remain independent candidate portfolios but form one atomic
release unit: both plans and attempts are bound before replay, neither member
can publish a terminal result alone, and four query-direction decisions are
released together. The prior 1,936 P5/P1 economic cells remain in the exposure
ledger; the P4 pair adds another 1,936. The selector still has no p-value, so a
positive result can be called only a Discovery `SCREENING_SURVIVOR`.

Safe parallelism is limited to:

- Bounded independent date/contract cache construction, including that
  partition's verification and hashing

Unsafe parallelism includes time-slice replay, per-occurrence raw scans,
scenario/direction/contract replay shards, per-cell raw scans, admitting a
later date before all prior-date state commits, and merging independently
simulated occupancy histories. Pure vectorized arithmetic inside one ordered
event step is allowed, but it does not create another logical replay pass.
Candidate campaigns also remain ordered when the research plan declares an
explicit first and second candidate.

---

## 8. Cost Model

Each trade must include:

```text
broker_or_fcm_commission
exchange_and_regulatory_fees
platform_routing_fee
spread_and_fill_price_effect
slippage
allocated_market_data_and_api_cost
allocated_license_cost
applicable_account_fees
```

Do not double count spread or slippage already represented in the fill price.

Report:

- Marginal net PnL using variable trading costs
- Fully loaded net PnL including allocated fixed operating costs

Final strategy decisions must include fully loaded results.

For `e2a_month_end_v1`, the primary research result uses executable-side BBO
fills and a separately verified actual fee schedule. It must not add synthetic
entry or exit adversity on top of BBO fills. Until the fee source and
verification date are frozen, the runner may reproduce the historical
BBO-plus-1.5-tick laboratory result but cannot label it actual measured cost.

Its fixed operating costs are allocated at portfolio level or over a
preregistered actual expected-fill denominator. The existing per-candidate
calendar-month lump allocation is inapplicable. Fourteen- and eighteen-tick
debits are diagnostic stresses only. These rules do not modify any frozen
existing campaign cost artifact.

---

## 9. Validation

Numeric criteria belong in `VALIDATION.md`.

```text
Discovery
    ↓
Five-fold walk-forward validation
    ↓
Independent stress
    ↓
Sealed holdout
    ↓
Paper eligibility decision
```

The named e2a path instead follows:

```text
independent historical reconstruction
    ↓
REGISTERED_RESEARCH_ONLY with every historical period marked in-sample
    ↓
content-addressed no-order shadow precommit
    ↓
12 prospective calendar events with no interim promotion look
    ↓
shadow diagnostics; broker-fill slippage remains NOT_OBSERVABLE
    ↓
separate policy and user decision before any true Paper request
```

The exact numeric and evidence rules are in `VALIDATION.md` Section 14A.

Required principles:

- Preserve time-series order.
- Apply train-defined regime thresholds unchanged to validation.
- Do not modify a strategy after viewing the sealed holdout.
- Record every trial.
- Preserve failed families.
- Do not use the same data as both calibration and independent evidence.

A bounded pilot may verify the pipeline and reject obviously uneconomic
hypothesis families before committing to a larger historical-data scope.

---

## 10. Strategy Registry

Store every experiment and strategy, regardless of outcome.

```text
experiment_id
strategy_id and parent_id
hypothesis_family
parameters
source data and code versions
train and validation intervals
metrics
cost model
execution assumptions
result = PASS | FAIL | INCONCLUSIVE
rejection_reason
lifecycle_state
created_at
```

Parameter variants may be registered as separate experiments and may become
separate strategy candidates when their behavior is materially different.
Variants produced by the same hypothesis family and data search are not
independent discoveries: preserve their shared lineage, count every variant
against the same trial budget, and apply multiplicity-aware validation.

---

## 11. Paper Eligibility

This section does not accept `e2a_month_end_v1` under the present policy.
Historical registration or a no-order shadow result creates no Phase 2 Paper
artifact. A future handoff requires a separately approved amendment resolving
the fixed-horizon strategy's broker-resident protection and risk controls.

A strategy artifact that passes `VALIDATION.md` is handed to Phase 2 with:

- Exact strategy artifact
- Backtest, out-of-sample, and stress results
- Expected trade frequency and holding time
- Take-profit-first, stop-first, roll/terminal-exit, and censored counts
- Time-to-hit and capital-occupancy distributions
- Expected costs and slippage
- Take-profit, stop-loss, potential profit, planned loss, and reward-to-risk
- Required Live features, depth, and freshness
- Initial risk proposal

---

## 12. Deliverables

- Experiment registry
- AI data-exploration and hypothesis interface
- One-second feature and five-minute research-table builders
- Feature builder
- Content-addressed date/contract event-cache builder and manifest
- Deterministic event-driven backtester
- Chronological replay checkpoints and exact-resume verifier
- Execution and cost models
- Validation runner
- Strategy artifact builder
- Paper eligibility package
- Research reports

---

## 13. Completion Criteria

- AI proposals become preregistered experiments.
- Every backtest is reproducible.
- Future-leakage tests pass.
- Cost and execution models are applied.
- Raw-source open-count tests prove that replay does not scan MBP-10 per
  occurrence, scenario, direction, or barrier cell.
- Cache construction, chronological replay, checkpoint-resume, and independent
  uninterrupted-versus-resumed equivalence gates in `VALIDATION.md` pass.
- Every registered scenario/direction/contract has all 484 logical occupancy
  states. The p5 detail ledger emits all 1,613,172 records and the p1_05 ledger
  emits all 1,369,236; each aggregate result emits exactly 2,904 unique
  scenario/direction/take-profit/stop-loss summaries with complete variable,
  occupancy, and censoring lineage.
- MBO-only features do not enter the MBP-10 research path.
- Failed trials are preserved.
- At least one strategy becomes Paper-eligible, or every preregistered family
  fails conclusively.

---

## 14. Remaining Implementation Questions

- Measured per-worker peak memory and a machine-specific memory budget; the
  current Phase 1A worker and in-flight ceiling remains four
- Feature storage schema
- Whether an ML library is needed
- Synthetic stress set
