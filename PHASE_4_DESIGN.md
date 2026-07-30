# Phase 4 Design: Risk and Capital Management

- Document version: 1.6.0-draft
- Status: `DRAFT`
- Parent document: [`DESIGN.md`](DESIGN.md)
- Type: Cross-cutting design

---

## 1. Objective

Phase 4 defines risk and capital controls across Research, Paper, Controlled
Live, and scaling.

Risk management is not added after Live trading:

```text
Phase 1: Backtest risk assumptions
Phase 2: Paper Risk Engine and fault tests
Phase 3: Live enforcement
Phase 5: Ongoing allocation and demotion
```

---

## 2. Risk Authority

The Risk Engine takes precedence over Signal and Execution:

```text
Signal proposes a target
    ↓
Risk approves, resizes, or rejects
    ↓
Execution computes the order delta
```

No order may reach the broker without risk approval.

The Risk Engine exposes no runtime operation that weakens an active limit or
clears a latched breach outside the change-control rules in `DESIGN.md`.

`VALIDATION.md` exclusively owns numeric thresholds. Phase 4 owns the meaning,
evaluation order, state transitions, and enforcement behavior of those
thresholds. Phase 3 consumes the Phase 4 controls and records their Live
outcomes.

---

## 3. Risk Categories

### Market and data risk

- Stale or missing market data
- Book corruption
- Abnormal spread or depth
- Halted or closed markets
- Contract or expiry uncertainty
- Clock or cutoff uncertainty

### Strategy risk

- Stop distance
- Maximum holding time
- Expected and stressed loss
- Drawdown
- Regime failure
- Correlation and concentration

### Execution risk

- Unknown order
- Partial fill
- Cancel/replace race
- Slippage
- Missing protection
- Position mismatch

### Account and operational risk

- Margin
- Broker connectivity
- Duplicate writer
- Manual or external order
- Credential or session conflict
- Delivery risk

---

## 4. Pre-Trade Gate

Before a new entry or exposure increase, verify:

### Data

- Required streams are connected.
- Required depth is available.
- Freshness is within the strategy limit.
- No confirmed data loss exists.
- Trading status is open.
- The active contract is confirmed.

### Strategy

- Strategy state permits trading.
- The signal has not expired.
- The feature version matches.
- Holding horizon fits session and expiry limits.

### Account

- Broker position is reconciled.
- No unknown order exists.
- Margin state is known.
- Daily and cumulative losses are below limits.
- Capital allocation is available.

### Execution

- Exactly one automated Production writer is active.
- The selected platform matches.
- The order type is permitted.
- Take-profit and stop-loss prices are defined.
- The entry supports an attached broker-managed OCO bracket.
- The intent is not a duplicate.

Any uncertainty fails closed.

---

## 5. Position Sizing

Initial Live size is one 6E contract.

```text
planned_profit_at_target =
    take_profit_distance × tick_value
  - entry_slippage_stress
  - target_exit_slippage_stress
  - commissions_and_fees

stressed_loss_per_contract =
    stop_distance × tick_value
  + entry_slippage_stress
  + exit_slippage_stress
  + commissions_and_fees
  + gap_and_halt_buffer

reward_to_risk =
    planned_profit_at_target / stressed_loss_per_contract
```

`planned_profit_at_target` is an upside amount at the target, not expected
value or a guaranteed outcome. The probability model remains part of Phase 1.
All three values must be present before entry; their numeric acceptance
thresholds belong in `VALIDATION.md`.

Available capital must satisfy:

- Stressed-loss requirements
- Broker margin
- Maximum margin utilization
- Operational reserve

Reject the trade at zero contracts if the risk budget cannot support one.
Never round zero up to one.

Do not narrow the stop arbitrarily to fit the account. Strategy logic and
market behavior determine stop distance.

---

## 6. Loss Limits

Exact values belong in `VALIDATION.md`.

Required categories:

- Per-trade stressed loss
- Daily realized loss
- Weekly loss
- Strategy cumulative Live loss
- Project cumulative Live loss
- Maximum drawdown
- Consecutive losses
- Margin utilization

Possible breach actions:

```text
warn
reduce or block new entries
cancel unsafe entry orders
maintain or restore protection
exit safely when required
pause the strategy
require review before restart
```

A breach remains latched until its configured recovery transition completes.

---

## 7. In-Position Risk

Every strategy defines:

- Take-profit target
- Hard stop-loss
- Maximum holding time
- Early-exit conditions
- Session and weekend handling
- Roll and expiry handling

Account-safety rules take precedence over strategy rules.

### Protection

Prefer broker-resident protection or protection proven persistent through a
disconnect test.

- Match protective quantity to filled quantity.
- Keep take-profit and stop-loss siblings linked as OCO.
- Handle partial fills.
- Verify protection first after restart.
- Block new entries while protection is unknown.

---

## 8. Order Uncertainty

### Unknown submit

An unconfirmed submit result is `UNKNOWN`.

- Do not blindly retry.
- Query the broker.
- Reconcile execution and order IDs.
- Block exposure increases until resolved.

### Cancel

A cancel is not always risk-reducing. Cancelling protection can increase risk.
Check the order role and post-cancel protection state.

### Reversal

Do not cross directly from long to short or short to long:

```text
flatten
reconcile
new decision
new risk approval
new entry
```

---

## 9. Data and Platform Outages

Distinguish:

- Market data down, order channel up
- Order channel down, market data up
- Both down
- Broker state uncertain
- Protection uncertain

Default rules:

- Block new entries.
- Preserve broker-resident protection.
- Do not flatten automatically using stale data alone.
- Check broker state before an action that could duplicate an exit.
- Do not hedge or fail over automatically on another platform.
- Treat an unprotected position as the emergency priority.

---

## 10. Expiry and Delivery Avoidance

Physical delivery is not intended.

- Confirm the active contract and expiry.
- Confirm broker closeout and CME last-trade deadlines.
- Ensure maximum holding time plus exit buffer ends before the deadline.
- Flatten positions and working orders before the deadline.
- Block new entries when expiry is uncertain.

Set the exact safety buffer after measuring broker and operating timelines.

---

## 11. Kill Switches

Minimum controls:

```text
STOP_STRATEGY
STOP_NEW_ENTRIES
STOP_ALL_NEW_ORDERS
CANCEL_ENTRY_ORDERS
SAFE_EXIT_POSITION
TRADING_DISABLED
```

Do not use `CANCEL_ALL` as the default because it can remove protection.
Classify orders by role and audit every emergency action.

---

## 12. Capital Allocation

Scaling eligibility requires:

- Statement-corrected Live profitability
- Stable drawdown and slippage
- Sufficient trades and active days
- Low operational incident rate
- Acceptable correlation with existing strategies
- Measured size-dependent market impact

Scaling rules:

- Moving from one contract to two doubles exposure.
- Increase one step at a time.
- Require an observation period at every step.
- Reduce or pause faster than promotion when performance deteriorates.

Phase 4 produces the allocation proposal. Phase 5 owns the lifecycle
transition, subject to the authorization rule in `DESIGN.md`.

---

## 13. Risk Evidence

Every decision must link to:

```text
strategy_id
signal_id
broker position snapshot
open order snapshot
market data health
active contract
risk limits
stressed loss calculation
approved target
reason codes
decision timestamp
```

The system must trace each risk decision to the resulting broker action.

---

## 14. Deliverables

- Pre-trade Risk Engine
- Position-sizing module
- Loss-limit manager
- Protection monitor
- Order-uncertainty handling
- Expiry and delivery gate
- Kill-switch service
- Capital-allocation report
- Risk audit trail

---

## 15. Completion Criteria

Before Paper:

- Pre-trade gates and basic limits work.
- Duplicate and unknown-order tests pass.
- Protection logic can be tested in Paper.

Before Live:

- One-contract stressed loss and capital requirements are fixed.
- Daily and cumulative loss limits are fixed.
- Broker protection and emergency paths are verified.
- Position reconciliation passes.
- Expiry gates work.

Before scaling:

- Portfolio and correlation limits exist.
- Capital-allocation rules exist.
- Size-dependent impact is monitored.

---

## 16. Enforcement Questions

- Emergency exit policy by outage type
- Delivery safety buffer
- Multi-strategy netting and attribution
- Boundaries for risk-triggered automatic demotion

Phase 4 consumes the numeric inputs defined by `VALIDATION.md`. Measured Phase
2/3 evidence may support a proposal, but cannot change them directly.
