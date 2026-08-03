# Phase 1 Design: AI Research and Backtesting

- Document version: 1.7.0-draft
- Status: `DRAFT`
- Parent document: [`DESIGN.md`](../DESIGN.md)
- Input: Historical MBP-10 from the Data Source
- Governing documents: [`VALIDATION.md`](../VALIDATION.md),
  [`RESEARCH_PLAN.md`](../RESEARCH_PLAN.md)

---

## 1. Objective

Phase 1 lets AI explore Discovery data through reproducible computations and
turns measured patterns into executable, realistically tested bracket
strategies.

```text
Verified MBP-10
    ↓
one-second features and five-minute research rows
    ↓
AI-directed exploration
    ↓
Registered experiment
    ↓
directional entry, take-profit, and stop-loss policy
    ↓
Deterministic backtest
    ↓
Validation and stress tests
    ↓
Reject or mark Paper-eligible
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
- Five-minute closed buckets are the default AI discovery rows and signal
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

## 5. AI Research Loop

### AI may inspect and propose

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

### AI may not

- Modify `VALIDATION.md`
- Reuse the sealed holdout
- Delete trial counts or failed results
- Remove trading costs
- Bypass experiment registration or validation
- Inspect sealed-holdout features, labels, trades, or aggregate results before
  the artifact is frozen
- Treat AI-visible chronological slices as independent evidence

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
take-profit and stop-trigger rules
stop execution and OCO rules
barrier observation and terminal exit policies
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
- Every simulated entry applies the artifact's take-profit, stop-loss, OCO,
  and terminal risk/roll rules.
- Barrier results are determined from executable-side MBP-10 event order, not
  from five-minute high/low ordering.
- A take-profit touch is not a fill unless the registered execution model
  permits the fill. A stop price is a trigger, not a guaranteed fill.
- Only one open position per strategy and contract is permitted in the initial
  research scope; signals while occupied are recorded but do not create
  overlapping exposure.

An exact queue model is not accepted as Production evidence under the current
scope.

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
- Deterministic event-driven backtester
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
- MBO-only features do not enter the MBP-10 research path.
- Failed trials are preserved.
- At least one strategy becomes Paper-eligible, or every preregistered family
  fails conclusively.

---

## 14. Implementation Questions

- Backtester partitioning
- Feature storage schema
- Whether an ML library is needed
- Synthetic stress set
