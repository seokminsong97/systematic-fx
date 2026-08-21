# Phase 2 Design: Live Platform Evaluation and Paper Trading

- Document version: 1.8.0-draft
- Revised: 2026-08-20
- Status: `DRAFT`
- Parent document: [`DESIGN.md`](../DESIGN.md)
- Candidate A: IBKR Pro with CME market data
- Candidate B: AMP Futures with the Rithmic API
- Input strategies: Phase 1 Paper-eligible artifacts

---

## 1. Objective

Phase 2 has two objectives:

1. Select one platform for Production trading.
2. Run backtest-qualified strategies in Paper Trading and create the evidence
   required for Live approval.

Platform comparison is temporary. Only one platform remains in the Production
path after selection.

---

## 2. Selection Question

> When a strategy must decide, which platform delivers the required 6E market
> depth and trade information more quickly and reliably and handles orders
> more safely?

Cost principles:

- Select IBKR when it meets the required quality and speed.
- Justify Rithmic's added cost only with a measured improvement needed for
  strategy economics or safety.
- Do not weaken validation standards to choose the cheaper platform.
- Do not choose the more expensive platform based on reputation alone.

---

## 3. Confirmed IBKR Documentation

### Market depth

The TWS API requests Level 2 order-book data through `reqMktDepth`.

- `isSmartDepth=false`: direct-routed depth
- `isSmartDepth=true`: SMART aggregated depth

Market-depth callbacks provide:

```text
position
operation = insert | update | delete
side
price
size
marketMaker, when applicable
```

IBKR states that market depth is sent without sampling or filtering, but it
does not guarantee that every price is displayed.

### Live subscription

Real-time API data requires the appropriate subscription and API
acknowledgement. Do not assume that free data visible in TWS grants the same
API entitlement.

### Not established by documentation

- CME-event-to-API-callback latency SLA
- Exchange timestamp on every callback
- Exchange sequence
- Snapshot completion marker
- Stable depth-level count for 6E
- Callback queue behavior during bursts
- Suitability for required one-second feature aggregation and a five-minute
  signal decision

Do not equate published top-of-book update intervals with Level 2 delivery
semantics. Measure IBKR before accepting or rejecting it.

---

## 4. Rithmic Questions

Confirm through onboarding and capture:

- Whether entitlement provides MBO or aggregated depth
- Depth operations and snapshot/reset behavior
- Sequence and timestamp semantics
- Market-data, order, and account channels
- Reconnect and recovery behavior
- Submit, acknowledgement, cancel, and fill callbacks
- Reconciliation with AMP statements and commissions
- Total API, user, market-data, and routing cost

Do not assume exchange-direct, zero-latency, or exact queue data before
verification.

---

## 5. Comparison Environment

Run both collectors on the same machine when practical, or on equivalent
machines in the same region.

Required controls:

- Same active 6E contract
- Same measurement period
- Same clock synchronization source
- UTC and monotonic timestamps recorded immediately at callback entry
- Raw provider payload and callback order preserved
- Equivalent process priority and logging overhead
- Overlapping Databento reference data when available

Disclose any topology differences.

---

## 6. Live Data Capture

Capture at least:

- Market depth
- BBO or Level 1
- Trades
- Contract details
- Trading hours and status
- Connection, error, and reset events
- Callback entry timestamps

Do not invent unavailable fields:

- Record local callback time when no exchange timestamp exists.
- Do not create a synthetic exchange sequence.
- Do not infer snapshot completion from silence.

---

## 7. Comparison Metrics

### Availability and latency

- First callback time
- Signed relative arrival between candidates
- Callback jitter
- One-second feature and five-minute signal-bucket completion delay
- Stale duration
- Burst and queue lag

If absolute exchange latency is unknown, report relative timing against the
other candidate and reference feed without presenting it as absolute truth.

### Book quality

- BBO price agreement
- L1-L10 price agreement
- L1-L10 aggregate-size differences
- Available depth levels
- Crossed or invalid books
- Missing updates and recovery
- Readiness time after reset

### Strategy impact

- Feature differences
- Signal direction and timing differences
- Missed trading opportunities
- Candidate-specific simulated and Paper net results

Tail delays matter when they destroy important trades, even if average latency
is low.

---

## 8. Order-Path Evaluation

Verify in Paper or simulator environments:

- Contract resolution and margin preview
- Submit and acknowledgement
- Partial fills and fills before acknowledgement
- Cancel request, acknowledgement, and rejection
- Replace behavior
- Late fills
- Attached take-profit and stop-loss acceptance
- OCO sibling cancellation
- Protective quantity updates after partial fills
- Parent acceptance with child rejection
- Recovery of open orders and positions after reconnect
- Duplicate-submission prevention
- Broker-resident protection

Paper fills are not final evidence of Live profitability. Paper validates state
management and operational paths.

---

## 9. Paper Trading

### Shadow forward is not Paper Trading

A process that consumes contemporaneous data and records a frozen signal plus
hypothetical BBO fills without submitting an order is `SHADOW_FORWARD`. It
creates no external order, fill, position, protection, cancellation, or
recovery evidence and is not Paper Trading under this document.

The present `e2a_month_end_v1` amendment authorizes only that no-order shadow
mode. Its ledger may be made ready for later broker observations, but an empty,
synthetic, assumed, or model-derived slippage value cannot stand in for
broker-observed Paper fill slippage. Shadow events therefore cannot satisfy the
complete 12-event forward gate, the Paper counts in `VALIDATION.md`, or any Live
approval requirement.

The existing fixed 24-hour, no-stop e2a research artifact conflicts with the
broker-managed bracket requirement below. This document intentionally preserves
that conflict. Before any broker Paper order may be submitted, a separate
approved amendment must define broker-resident protection, timed-exit behavior
during disconnects, partial-fill handling, forced contract/gap exits, Phase 4
risk limits, platform integration, and explicit promotion authority. Until
then, no process may label e2a as `ENTER_PAPER` or `PAPER`, invoke a broker Paper
adapter for it, or submit an order on its behalf.

### True broker Paper entry and execution

Phase 2 accepts Paper-eligible artifacts from Phase 1. Run the same strategy
on both candidates over the same period when possible. Evaluate
provider-specific strategies separately.

The following requirements govern true broker Paper Trading and are not relaxed
by a historical or shadow-forward result.

Every exposure-increasing Paper entry must be submitted as a broker-managed
bracket:

```text
parent entry
    ├─ take-profit child
    └─ stop-loss child
         linked as OCO
```

Before submission, record:

```text
take_profit_price
stop_loss_price
potential_profit_ticks_and_currency
planned_loss_ticks_and_currency
reward_to_risk_ratio
terminal_exit_policy
roll_and_expiry_cutoff
```

The target and stop must come from the exact strategy artifact and Phase 4
risk configuration. Do not activate the parent when either child is missing,
rejected, or not linked. Child quantities must track broker-reported parent
fills, including partial fills.

### Required record

```text
strategy_id and version
platform
start and end dates
active trading days
signals generated
orders submitted
filled, partial, cancelled, and rejected orders
entry and exit types
holding time
estimated gross and net PnL
slippage estimate
maximum drawdown
consecutive losses
risk rejections
connection and recovery incidents
parent, take-profit, and stop-loss order IDs
bracket and OCO state transitions
```

### What Paper can establish

- Strategy operation on a Live event flow
- Timely availability of required data
- Signal and order-pipeline stability
- Correct risk rejection
- Order-state and position recovery
- Correct bracket, OCO, and partial-fill state transitions

### What Paper cannot establish

- Real fill probability
- Real slippage
- Real queue position
- Final fees
- Every broker behavior during a real outage
- Realized take-profit and stop-loss fill quality

---

## 10. Platform Selection

### Blocking criteria

A candidate may be rejected if it cannot:

- Deliver required depth reliably
- Detect and recover from data loss and resets
- Meet the required decision interval
- Reconcile orders and positions deterministically
- Maintain the required bracket and OCO semantics
- Provide verified automation and data-use permissions
- Provide safe emergency access

### Selection rules

Select IBKR when:

- Data accuracy and timing pass.
- Order-path and Paper-operation safety pass.
- Rithmic's measured improvement is not material to Live economics.

Select Rithmic when:

- IBKR fails required timing or data quality.
- Rithmic's improvement is repeatable.
- Fully loaded strategy economics remain valid after added fixed and routing
  costs.

### Decision

```text
SELECT_IBKR
SELECT_RITHMIC
REJECT_BOTH
INSUFFICIENT_EVIDENCE
```

Set a cost and time bound before collecting more evidence for an
`INSUFFICIENT_EVIDENCE` result.

After selection:

- Repeat Phase 2 before changing platforms.

---

## 11. Live Approval Package

Before Phase 3, submit:

### Strategy evidence

- Strategy and hypothesis
- Features, signal interval, bracket policy, and terminal exit logic
- Historical train and OOS periods
- Backtest trades, net PnL, EV, and drawdown
- Stress and failure cases

### Paper evidence

- Paper period and active trading days
- Signal and order counts
- Fill, cancel, and reject distribution
- Estimated PnL and drawdown
- Risk blocks and incidents

### Platform evidence

- Latency comparison
- Depth, BBO, and feature differences
- Order-path and cost comparison
- Selection and rationale

### Live proposal

- One 6E contract
- Entry policy, take-profit, stop-loss, and terminal roll/expiry policy
- Take-profit/stop-first counts and time-to-hit distribution
- Potential profit, planned loss, and reward-to-risk ratio
- Daily and cumulative loss limits
- Emergency exit path
- Minimum Live evidence
- Stop conditions

Numeric risk values must come from `VALIDATION.md`; their enforcement
configuration must conform to Phase 4.

Phase 2 owns this evidence package, not the authorization decision.

---

## 12. Deliverables

- IBKR and Rithmic market-data collectors
- Raw capture store
- Cross-platform comparison report
- Paper execution adapters
- Order-state golden tests
- Strategy Paper reports
- Platform selection report
- Live approval package

---

## 13. Completion Criteria

- Actual 6E data from both candidates is compared.
- Strategy-relevant timing and quality differences are measured.
- Paper order and recovery paths are verified.
- At least one platform passes the selection criteria.
- Exactly one platform is selected.
- A Live approval package is created.

---

## 14. Open Questions

- Actual IBKR 6E Level 2 callback latency
- Whether IBKR continuously provides the required ten levels
- Whether Rithmic entitlement is MBO or aggregated depth
- Reference source for absolute latency
- Platform-score weights and exact pass thresholds
- Minimum Paper active days and order count
- Permitted use of Paper fill models
- Total Rithmic monthly cost and session limits

Resolve these through documentation and measured capture.

---

## 15. Official IBKR References

- Market depth:
  https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/market-depth-l-2/introduction
- Request market depth:
  https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/market-depth-l-2/request-market-depth
- Receive market depth:
  https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/market-depth-l-2/receive-market-depth
- Update frequency:
  https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/top-of-book-l-1/market-data-update-frequency
- Subscriptions:
  https://www.interactivebrokers.com/docs/general/market-data-subscriptions/introduction
- TWS versus API data:
  https://www.interactivebrokers.com/docs/general/market-data-subscriptions/tws-data-vs-api-data
- API acknowledgement:
  https://www.interactivebrokers.com/docs/general/market-data-subscriptions/compliance-requirements-for-api-market-data/market-data-api-acknowledgement
