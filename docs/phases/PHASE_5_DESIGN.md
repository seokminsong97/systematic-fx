# Phase 5 Design: Continuous Strategy Lifecycle

- Document version: 1.5.0-draft
- Status: `DRAFT`
- Parent document: [`DESIGN.md`](../DESIGN.md)
- Input: Research, Paper, and Live evidence

---

## 1. Objective

Phase 5 continuously explores new strategies and manages existing strategies
according to measured performance.

New strategies cannot bypass existing gates.

---

## 2. Strategy States

```text
RESEARCH
BACKTEST_PASSED
PAPER
LIVE_APPROVED
CANARY
ACTIVE
SCALED
REDUCED
PAUSED
REVALIDATING
RETIRED
REJECTED
```

---

## 3. AI Authority

AI may:

- Propose new hypotheses
- Analyze strategy failures
- Generate challenger strategies
- Propose features and regimes
- Propose revalidation plans
- Recommend promotion or demotion

AI outputs proposals only. The transition authority in Section 6 determines
whether a proposal changes lifecycle state.

---

## 4. Continuous Research Loop

Run on a schedule or in response to events:

1. Summarize performance and regimes.
2. Create bounded research requests under `RESEARCH_PLAN.md`.
3. Route experiments through Phase 1.
4. Route qualifying artifacts through Phase 2.
5. Collect downstream evidence and propose state transitions.

Maintain trial counts to control false discovery from repeated exploration.

---

## 5. Performance Monitoring

Track per strategy:

- Gross, marginal net, and fully loaded net PnL
- Rolling EV
- Win/loss distribution
- Maximum and current drawdown
- Consecutive losses
- Trades and active days
- Holding time
- Slippage
- Fill and reject rates
- Risk-rejection rate
- Data and platform incidents
- Performance by regime
- Correlation with other active strategies

Report Backtest, Paper, and Live results separately.

---

## 6. Transition Authority

| Transition | Required evidence | Authority |
|---|---|---|
| `RESEARCH → BACKTEST_PASSED` | Phase 1 validation pass | Automatic |
| `BACKTEST_PASSED → PAPER` | Phase 1 deployment package | Automatic |
| `PAPER → LIVE_APPROVED` | Phase 2 approval package | User |
| `LIVE_APPROVED → CANARY` | Passing Phase 3 activation gate | Automatic |
| `CANARY → ACTIVE` | Phase 3 Live evidence | Lifecycle policy |
| `ACTIVE → SCALED` | Phase 4 allocation proposal | User |
| `PAUSED → PAPER/CANARY` | Completed revalidation | User |

---

## 7. Demotion and Pause

### Automatic safety pause

A blocking Phase 3 incident or Phase 4 breach transitions the strategy
immediately to `PAUSED`. The originating phase owns the detailed reason code.

### Performance demotion

Scheduled review may recommend demotion for:

- Deteriorating rolling EV
- Approaching drawdown limits
- Increased slippage
- Regime breakdown
- Increased correlation or concentration
- Persistent risk rejection

Safety reductions and pauses may be automatic. Recovery follows the transition
table in Section 6.

---

## 8. Revalidation

Prior evidence cannot be carried forward unchanged after:

- Historical normalization or data-semantics changes
- Feature-semantics changes
- Model or strategy-logic changes
- Live adapter or data-semantics changes
- Platform changes
- Execution or fill-model changes
- Stop, sizing, or risk-semantics changes

Return to the earliest affected stage:

- Feature or model change: Phase 1
- Live adapter or platform change: Phase 2
- Live risk or execution change: Phase 2 Paper or Phase 3 Canary
- Historical-data change: Phase 1 and all downstream validation

Never return directly to `ACTIVE`.

---

## 9. Strategy Registry and Lineage

Never delete strategy history.

```text
strategy_id
parent_strategy_id
hypothesis
versions
research experiments
backtest results
Paper deployments
Live approvals
Live deployments
performance history
risk incidents
state transitions
rejection or retirement reason
```

Do not overwrite an artifact under the same identity. A semantic change creates
a new version or strategy ID.

---

## 10. Portfolio Interaction

When multiple strategies are active, manage:

- Net directional exposure
- Strategy correlation
- Concurrent positions
- Shared drawdown
- Capital allocation
- Conflicting signals
- PnL attribution

Initially, limit simultaneous Live strategies or positions to one when needed
to control complexity. Expand only after evidence supports it.

---

## 11. Review Cadence

Set exact cadence after operating experience. Required review types:

- Daily operational review
- Weekly strategy-performance review
- Monthly fully loaded cost and portfolio review
- Immediate incident review
- Major data or platform-change review

Every review records its evidence and state-transition recommendation.

---

## 12. Deliverables

- Lifecycle state manager
- Strategy registry
- Automated research queue
- Paper deployment scheduler
- Live approval report generator
- Performance monitor
- Promotion and demotion recommendation engine
- Revalidation planner
- Portfolio-allocation report

---

## 13. Completion Criteria

Phase 5 is an ongoing operating capability. Initial capability is complete
when:

- New hypotheses can enter the Phase 1 pipeline.
- Validation-passing strategies deploy automatically to Paper.
- Paper evidence can produce a Live approval report.
- Authorization-gated transitions enforce Section 6.
- Live performance is tracked by strategy.
- Safety pause and demotion paths work.
- Retired and rejected strategy history is preserved.

---

## 14. Open Questions

- Research-generation cadence
- Performance-review cadence
- Policy for performance-triggered automatic demotion
- Repromotion criteria
- Multi-strategy concurrency
- Portfolio-allocation algorithm
- Maximum AI experiment budget
- Maximum active-strategy count

Resolve these after enough strategies and Live operating evidence exist.
