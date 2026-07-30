# Phase 3 Design: Controlled Live Trading

- Document version: 1.6.0-draft
- Status: `DRAFT`
- Parent document: [`DESIGN.md`](DESIGN.md)
- Prerequisite: Phase 2 completion

---

## 1. Objective

Phase 3 validates the strategy and execution system with minimal real capital
on one selected platform.

Controlled Live does not maximize profit. It answers:

- Does real data timing match expectations?
- How do real orders and fills differ from Paper assumptions?
- Do real slippage and fees preserve the edge?
- Are protection and recovery safe in the real account?
- Does a Backtest- and Paper-qualified strategy work in the market?

---

## 2. Entry Criteria

All are required:

- Phase 1 validation pass
- Phase 2 Paper evidence
- Valid Live authorization record matching the exact strategy, platform,
  account, and risk configuration
- Phase 4 Live risk controls
- Verified Production account, permissions, and data entitlements
- Verified manual emergency access
- Verified broker-resident protection
- Passing open-order and position-reconciliation tests

---

## 3. Initial Scope

- Product: 6E
- Quantity: no more than one contract

Do not start Live trading if available capital cannot support one contract
under stressed loss and margin requirements.

---

## 4. Activation Sequence

```text
1. Verify selected platform and account.
2. Verify strategy artifact and code version.
3. Verify risk configuration.
4. Query open orders, executions, and position.
5. Confirm position is flat.
6. Confirm no unknown or external orders.
7. Verify market-data health and active contract.
8. Verify the protective-order path.
9. Verify all entry criteria.
10. Enable Production order permission.
```

Any failure leaves the system read-only or trading-disabled.

---

## 5. Order and Position Safety

### Single writer

Exactly one automated process may submit Production orders. Research, AI,
dashboards, and signal components do not receive broker credentials. Manual
emergency actions are permitted only through the approved interface; the
automated writer must detect and reconcile them before resuming.

### Broker truth

The broker is the final operational source of truth for:

- Open orders
- Executions
- Intraday position
- Account and margin state

### Unresolved broker state

A submit timeout or ambiguous callback becomes `UNKNOWN`. Any unresolved order
or position mismatch blocks new exposure.

- Do not issue another submit for the same intent while it is unresolved.
- Query broker open/completed orders and executions.
- Reconcile internal state against broker state.
- Block new exposure until order identity and position are resolved.

### Reversal

Apply the Phase 4 flatten-and-reconcile reversal policy and audit each leg.

---

## 6. Protection

Phase 3 verifies the Phase 4 protection control against the selected broker.
Every exposure-increasing Live entry must use the same broker-managed
take-profit/stop-loss OCO bracket qualified in Phase 2. The parent must not
become active unless both children are accepted and linked. The Live checks
cover persistence through disconnects, quantity after partial fills, OCO
behavior, and protection recovery after restart. Unknown protection follows
the Phase 4 emergency transition and creates an incident record.

A stop does not guarantee maximum loss; stress gaps and slippage separately.

---

## 7. Controlled Live Evidence

Record for each trade:

```text
strategy_id and version
platform and account
signal timestamp
feature snapshot
risk decision
broker submit timestamp
acknowledgement timestamp
fill timestamps and prices
partial fills
cancel and replace events
protective order state
exit reason
estimated and statement fees
realized PnL
slippage versus expected execution
incidents
```

Aggregate:

- Active trading days, signals, and trades
- Win/loss distribution
- Gross and net PnL
- Maximum drawdown and consecutive losses
- Expected versus actual slippage and fees
- Submit, acknowledgement, fill, and cancel latency
- Protection gaps
- Data and operational incidents

---

## 8. Monitoring and Reconciliation

### Intraday

- Market-data freshness and connection state
- Open orders, position, and protection
- Margin
- Daily PnL and loss-limit usage

### Daily

- Internal ledger versus broker orders
- Executions and position
- Fee estimate and strategy PnL
- Incidents

### Statement

Use broker or FCM statements to finalize:

- Fills
- Commission and exchange/routing fees
- Realized PnL
- Position settlement

Strategy economics use statement-corrected results.

---

## 9. Stop Conditions

Any blocking Phase 4 decision stops new orders. Phase 3 additionally stops for
an uncertain broker-session owner, an artifact mismatch, or an operator pause.

Automatic stopping is allowed. Restart follows the Phase 5 recovery
transition.

---

## 10. Platform Failure

Execute the Phase 4 outage policy, verify account state through the selected
broker's manual interface, and record the incident. A platform change returns
to Phase 2.

---

## 11. Live Decision

Phase 3 does not define numeric risk thresholds. `VALIDATION.md` defines them,
Phase 4 provides their enforcement semantics, and Phase 3 applies the resulting
controls and records Live evidence.

```text
CONTINUE_CANARY
PROMOTE_TO_ACTIVE
PAUSE
RETURN_TO_PAPER
REVALIDATE_STRATEGY
REVALIDATE_PLATFORM
RETIRE_STRATEGY
```

Promotion requires:

- Sufficient Live trades and active days
- Positive statement-corrected economics
- Drawdown and losses within limits
- Slippage and fees within expected ranges
- Stable protection and reconciliation
- No blocking incident

Scaling is handled as a Phase 5 lifecycle transition.

---

## 12. Deliverables

- Production broker gateway
- Live activation checklist
- Order and position reconciliation
- Protection manager
- Emergency stop path
- Trade-level audit trail
- Daily and statement reports
- Controlled Live performance report
- Promotion or pause recommendation

---

## 13. Completion Criteria

Completion is evidence-based, not time-based:

- Minimum required Live evidence is met.
- Broker statements are reconciled.
- Protection and recovery are verified.
- Actual slippage and cost models are updated.
- A next-state decision is issued for the strategy and platform.

---

## 14. Operational Questions

- Overnight and daily-break policy
- Manual emergency interface and session policy
- Scaling interval after one contract

Phase 3 consumes the Live thresholds defined by `VALIDATION.md`; measured
evidence may support a later proposal to revise that document.
