# Phase 1 Design: AI Research and Backtesting

- Document version: 1.6.0-draft
- Status: `DRAFT`
- Parent document: [`DESIGN.md`](DESIGN.md)
- Input: Historical MBP-10 from the Data Source
- Governing documents: `VALIDATION.md`, `RESEARCH_PLAN.md`

---

## 1. Objective

Phase 1 turns AI-generated trading hypotheses into reproducible, realistic
experiments.

```text
AI hypothesis
    ↓
Registered experiment
    ↓
Feature and strategy implementation
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

- 1 second as the baseline
- 500 milliseconds as a challenger
- Promote 500 milliseconds only when measured value justifies the added cost
  and complexity

### Holding horizons

```text
15m | 30m | 1h | 2h | 4h
```

Strategies holding longer than four hours are out of scope.

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

### AI may propose

- Hypotheses
- Feature transformations
- Regime definitions
- Entry and exit rules
- Holding horizons
- Model families
- Bounded hyperparameter ranges
- Failure analyses and next experiments

### AI may not

- Modify `VALIDATION.md`
- Reuse the sealed holdout
- Delete trial counts or failed results
- Remove trading costs
- Bypass experiment registration or validation

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
holding_horizon
entry_and_exit_policy
cost_model
execution_model
train_validation_plan
random_seed, when applicable
parent_experiment_id
```

Changing a parameter boundary or success criterion after execution creates a
new experiment.

---

## 6. Strategy Artifact

A strategy is more than a model file. Its artifact contains:

```text
strategy_id
strategy_version
hypothesis
feature_set_version
signal and entry rules
take-profit, stop-loss, and maximum holding rules
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
  and maximum-holding-time rules.

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
Train
    ↓
Walk-forward validation
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
- Expected costs and slippage
- Take-profit, stop-loss, potential profit, planned loss, and reward-to-risk
- Required Live features, depth, and freshness
- Initial risk proposal

---

## 12. Deliverables

- Experiment registry
- AI hypothesis interface
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
