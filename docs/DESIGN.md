# CME 6E Systematic Trading System

- Document version: 1.9.0-draft
- Revised: 2026-08-20
- Status: `DRAFT`
- Documentation language: English
- Market: CME Euro FX Futures (`6E`), outright futures only
- Default deterministic discovery and signal interval: 5 minutes
- Intrabucket feature interval: 1 second where supported by the data
- Paper/Live exit model: price-barrier first touch with broker-managed
  protection; the named `e2a_month_end_v1` fixed-horizon path is governed
  historical and no-order shadow research only
- Live platform candidates: IBKR and AMP Futures with Rithmic

---

## 1. Purpose of This Document

This document owns the high-level system design:

1. What the project builds
2. The implementation phases and their order
3. The objective and completion criteria of each phase
4. Principles that implementation convenience cannot override
5. Where unresolved questions must be answered

Provider callbacks, order state machines, recovery procedures, IAM, signing,
and other implementation details belong in the relevant phase design.
Unverified provider behavior must not be treated as a fixed contract.

### Document authority

| Document | Authority |
|---|---|
| `DESIGN.md` | Project objective, scope, phase order, and governing principles |
| `PHASE_N_DESIGN.md` | Implementation, tests, deliverables, and open questions for that phase |
| `VALIDATION.md` | Numeric criteria for rejection, Paper entry, Live approval, and performance |
| `RESEARCH_PLAN.md` | Hypothesis families, experiment scope, priorities, and trial budget |

Lower-level documents cannot change this document's direction. Amend
`DESIGN.md` first when the project direction changes.

---

## 2. Project Objective

The project has one ultimate objective:

> Build an automated trading system that produces sustainable positive net
> returns after commissions, exchange and routing fees, spread, slippage,
> market-data and API costs, and operating expenses.

A statistically interesting pattern, high predictive accuracy, or positive
gross return is not sufficient.

### In scope

- CME `6E` only
- Actual expiry contracts
- Deterministic, finite-budget discovery on 5-minute feature rows built from
  verified MBP-10
- One-second intrabucket features when they preserve economically useful book
  dynamics
- Event-level MBP-10 execution and first-touch validation
- Long and short strategies that emit an executable entry, take-profit, and
  stop-loss bracket
- Price-barrier exits without an alpha-imposed maximum holding period; duration,
  capital occupancy, weekend exposure, and time to each exit remain measured
- Historical research using verified MBP-10 data
- Governed reconstruction and no-order prospective shadow evaluation of the
  single frozen `e2a_month_end_v1` calendar-event artifact under the boundary
  below
- Measured comparison of IBKR and Rithmic before selecting one Live platform
- An initial Live allocation of one 6E contract

### Out of scope

- M6E or other products
- HFT, market making, or colocation
- Simultaneous Live trading on two platforms
- Automatic broker failover
- AI authority to approve Live trading, relax risk, or increase capital
- LLM participation in the research daemon runtime control loop
- Research-daemon authority to open sealed holdout data or promote a candidate
  to Paper or Live
- Strategies that depend on unverified provider behavior
- Strategies whose result is only a forecast, score, or chart pattern and does
  not resolve to an executable bracket policy, except for the single named
  research-only fixed-horizon artifact governed below; that exception does not
  apply to broker Paper or Live trading

### Governed e2a research boundary

`e2a_month_end_v1` is the only calendar-event fixed-horizon artifact admitted
by this amendment. It may be independently reconstructed, registered as
governed historical research, and evaluated prospectively in
`SHADOW_FORWARD`. It must not be added to a generic candidate catalog, used to
reopen the closed search map, or varied on any historical period.

For this boundary, `SHADOW_FORWARD` means that contemporaneous decision inputs,
the frozen signal, and hypothetical BBO entry and exit fills are recorded
without submitting an order to any broker or simulator account. It creates no
broker order, position, fill, or recovery state. It is not Phase 2 Paper
Trading, does not enter the `PAPER` lifecycle state, and cannot satisfy a Paper
evidence requirement that depends on broker-observed fills or slippage.

All historical periods exposed for the month-end family are in-sample. In
particular, the 2026-02 through 2026-07 one-shot holdout is consumed and cannot
be described as fresh out-of-sample evidence. Historical reconstruction
verifies lineage and arithmetic; it does not restore holdout status.

This amendment does not alter Governing Risk Rules 5-7. The fixed 24-hour
research exit is not an exception to the broker-managed bracket requirement for
true Paper or Live entries. `e2a_month_end_v1` may not enter broker Paper or
Live until a separate approved amendment defines its broker-resident
protection, risk limits, timed-exit recovery behavior, promotion authority, and
user approval path.

---

## 3. System Flow

```text
Historical MBP-10 data
    ↓
Point-in-time event features and quote-aware first-passage labels
    ↓
Finite, precommitted deterministic search epochs
    ↓
Content-addressed date/contract event cache
    ↓
One shared chronological first-touch and execution backtest pass
    ↓
IBKR/Rithmic Live-data and order-path comparison
    ↓
Paper Trading
    ↓
Live approval package and user approval
    ↓
One-contract Controlled Live on one selected platform
    ↓
Promote / Scale / Reduce / Pause / Retire
```

Risk management is cross-cutting. A minimum Risk Engine must be complete
before Paper Trading and strengthened before Live trading and scaling.

### Research-daemon invariant

The research daemon may remain alive indefinitely without a human or LLM in
its runtime control loop. Each research epoch is nevertheless finite and must
freeze its dataset and feature/label/execution versions, code identity,
strategy families, parameter and barrier ranges, real/null budgets, seeds,
admission rules, and parent identity before the first candidate is queued. A
spent epoch may finish registered work, retries, and reporting, but it may not
generate more candidates or adapt its frozen search space after observing a
result.

The daemon's maximum authority is `REGISTER`. Search walk-forward output is
exploratory search-data evidence, not final OOS evidence. Sealed holdout data
must be absent from the daemon credential and storage namespace, and only a
separately authorized process may ever evaluate it. The daemon cannot unseal a
holdout, enter Paper, enter Live, or submit an order. An LLM may later propose
hypotheses outside this loop, but failure or absence of that optional proposer
cannot affect daemon health or progress.

M0b defines that authority boundary in PostgreSQL. One `m0b_epochs` row binds a
single frozen campaign to its canonical manifest and exact dataset, scheduled
CME calendar, contract reference, Discovery split, feature, label, execution,
cost, engine, dependency lock, code, retry ceiling, and finite REAL/NULL budgets.
Candidate RunSpecs must be `SCREEN` runs
with `SEARCH`/`DISCOVERY` parameters, and successful attempts, exact result
artifact links, and `SCREENED_OUT` or `REGISTERED` candidate states commit
together. Checkpoint chains and linked artifact bytes are append-preserved.
The database rejects RunSpecs before epoch registration, attempts before
candidate registration, lifecycle regression, unfinished epoch failure, and
post-terminal mutation; this prevents a generic ledger row from bypassing the
M0b authority boundary.
No M0b table or procedure represents holdout opening, Paper/Live eligibility,
or order authority. Candidate RunSpec, the immutable CandidateWork artifact
identity and the budget row must register atomically through
`register_m0b_candidate`.
Migration `0030` adds four narrowly scoped `SECURITY DEFINER` capabilities for
claim, checkpoint, terminal result and failure/retry; its actual-login verifier
accepts only that allowlist and rejects direct ledger DML. CandidateWork v2
binds the canonical candidate's volatility bracket to its exact rational label
identity, and binds the evaluation policy plus cost/execution/split hashes.
Lease tokens are hidden from worker reads and bound to the authenticated LOGIN;
a terminal result must equal the latest complete checkpoint's hash, size,
metrics, and DB-derived classification. The bounded
`research m0b worker-cycle` launcher resolves and fully verifies the claimed
CandidateWork hash, signal and first-passage store before advancing at most one
pre-registered candidate. Its owner-only durable lease token, heartbeat,
expired-attempt recovery and exact terminal/failure replay make the cycle
restart safe without granting candidate generation or promotion authority.
This is an operational finite worker boundary, not authorization for a real
performance epoch or an assertion that a production service is deployed.

The scheduled CME reference and trading-status evidence are distinct inputs.
A scheduled-open interval proves only that a known close is not crossed; it
does not prove the market was continuously tradable. Consequently a real M0b
label stays ineligible whenever status coverage is absent, even if its
quote-aware first-passage outcome can be computed for pipeline diagnostics.
Status evidence is evaluated as-of the event timestamp: an observation whose
publication timestamp is still in the future, has exceeded its frozen maximum
age, or falls outside the exact CME Globex 6E scope fails closed. A verified
OPEN snapshot authorizes only the entry-time status fact; subsequent status
transitions belong to chronological execution replay.
Full entry eligibility additionally requires both schedule and status archive
files to reopen with their separately frozen upstream-source hashes; a caller-
constructed evidence object or recurring-hours fallback cannot authorize entry.

The production calendar surface is likewise an immutable schedule archive,
not an extrapolation of recurring weekly hours. Each session revision carries
its publication timestamp, exact open/close and break intervals, source/file
identity, and bounded coverage. An as-of query may consume only a revision
published by that timestamp. Missing dates, overlapping intervals, or an
absent official archive fail closed; the narrow M0b mechanics reference is not
treated as multi-year holiday or emergency-closure evidence.

Schedule revisions are selected point-in-time (`published_ts <= event_ts`) and
entry eligibility requires the archive as-of time to equal the event time. The
same archive resolves the previous completed session for volume-based active
contract selection. Weekday subtraction is therefore only a bounded,
holiday-free fallback and cannot authorize a production epoch; scheduled
availability and operational status remain independent proofs.

Active-contract selection is a separate point-in-time fact. The policy sums
trade volume over the entire previous completed CME trading session across its
UTC source partitions and exposes the result only after that session closes.
It may honestly select a contract already inside its roll guard; the entry
eligibility layer then blocks the new trade. This separation prevents a risk
rule from silently rewriting historical active-contract evidence and preserves
the invariant that an accepted trade keeps its entry `instrument_id` to exit.

### Historical replay invariant

Phase 1 raw MBP-10 files are qualification and cache-construction inputs, not
per-signal replay inputs. A raw source file must never be rescanned once per
signal occurrence, stress scenario, direction, or barrier cell. Bounded
workers may build each required immutable, content-addressed date/contract
cache key below `data/derived` once and all later consumers reuse it by hash.
The Phase 1A p5 implementation additionally publishes a semantic request index
for each exact cache request. Its governed ceiling is four cache workers and
four in-flight cache keys, with exactly one cache key owned by each worker.
Raw-source identity in that index and in cache metadata is a portable
`data/`-relative URI, never an absolute workstation path. Source and cache
Parquet bytes are hashed and decoded through the same held file descriptor;
artifact paths are traversed with no-follow descriptor-relative opens and are
rechecked against their held inode before acceptance. A path replacement,
symlink swap, request/report mismatch, or same-size content change therefore
fails closed instead of becoming replay evidence.

The backtester then consumes the verified cache in one source-time-ordered pass
and updates every registered scenario, direction, contract, and barrier-cell
state from the same ordered events. Only independent date/contract cache
construction is parallel; economic replay remains one logical chronological
pass and is not sharded by time, scenario, direction, contract, occurrence, or
cell. A checkpoint or resumed attempt must remain bound to the exact RunSpec,
cache manifest, preceding checkpoint, code snapshot, and append-only result
artifacts in PostgreSQL.

Checkpoint recovery and final validation stream immutable daily detail shards
one at a time. They may not materialize the cumulative 1,613,172-row ledger in
memory; peak retained detail state is bounded by one source-date shard plus the
compact economic accumulators. Skipping shard decoding never skips integrity:
lineage-only and finalization paths still stream every referenced artifact
through SHA-256 using constant memory.

Source dates are admitted strictly in increasing order. Within one completed
source date, the canonical cross-contract event key is
`(ts_recv_ns, sequence, event_index, contract_key)`. This ordering, the worker
ceiling, and the actual runtime worker count are part of governed run lineage;
changing process scheduling must not change cache or result bytes.

The frozen calendar's nominal last pre-expiry partition is only a cache-request
boundary, not proof that it contains an executable terminal quote. After the
complete cache report set exists, the runner reverse-scans each contract's
eligible partitions and selects the latest partition with a valid executable
quote; absence of one anywhere before the expiry month is a hard failure. The
versioned resolution policy, complete per-contract selection, and its semantic
SHA-256 are recorded in the RunSpec. That SHA-256 is copied into every
checkpoint/final-result input lineage, while the cache-manifest hash binds the
valid-count and last-valid-event facts from which it was derived. Partitions
after a fallback terminal are still fully read for integrity verification, but
their invalid-only observations are excluded from the economic stream.

---

## 4. Data Source

[`mbo-mbp10-converter`](https://github.com/seokminsong97/mbo-mbp10-converter)

---

## 5. Development Phases

### Phase 1: Deterministic Research and Backtesting

Objectives:

- Run bounded, reproducible Discovery epochs without an LLM in the runtime
  control loop. Optional externally proposed hypotheses must be frozen before
  an epoch starts.
- Generate candidate hypotheses autonomously through a feature-only symbolic
  AI outside the worker. Its exact catalog, context, output budget, request,
  and implementation identity are frozen before scoring; it has proposal-only
  authority and cannot inspect labels or expand an epoch.
- Convert every candidate into a directional entry policy with take-profit and
  stop-loss prices.
- Implement and test each hypothesis reproducibly.
- Measure performance with realistic costs, latency, spread, slippage, and
  risk.
- Register only search-data survivors for a separately authorized holdout,
  forward-evidence, and promotion process.

The research daemon cannot enter Paper Trading. Holdout opening and Paper or
Live promotion are separate authorized actions; neither may be inferred from a
search-data result. No AI or daemon may change the frozen pass criteria.

Design: [`PHASE_1_DESIGN.md`](phases/PHASE_1_DESIGN.md)

### Phase 2: Live Platform Evaluation and Paper Trading

Objectives:

- Capture comparable 6E data from IBKR and Rithmic.
- Measure which platform delivers required MBP-10 information more quickly
  and reliably.
- Compare submit, acknowledgement, cancel, recovery, and operational safety.
- Run backtest-qualified strategies in Paper Trading.
- Select one Production platform based on measured cost and performance.

Default selection rule:

> Select IBKR when its data and order path meet the strategy requirements.
> Select Rithmic only when IBKR's measured limitations damage strategy
> economics or safety enough to justify the additional cost.

Design: [`PHASE_2_DESIGN.md`](phases/PHASE_2_DESIGN.md)

### Phase 3: Controlled Live Trading

Objectives:

- Trade only through the selected platform.
- Start with one 6E contract after explicit user approval.
- Measure actual fills, slippage, fees, protection, and recovery.
- Verify that Backtest and Paper results survive in the real market.

Design: [`PHASE_3_DESIGN.md`](phases/PHASE_3_DESIGN.md)

### Phase 4: Risk and Capital Management

Objectives:

- Apply risk controls across Research, Paper, Live, and scaling.
- Control losses, positions, orders, data failures, expiry, and operational
  failures.
- Permit capital growth only when supported by evidence and risk capacity.

Phase 4 is an independent, cross-cutting workstream. It runs in parallel with
the other phases and must deliver the required controls before each gate:

- Before Phase 2 Paper Trading: basic Risk Engine
- Before Phase 3 Controlled Live: protection, loss limits, and emergency paths
- Before Phase 5 scaling: portfolio and capital-allocation controls

Design: [`PHASE_4_DESIGN.md`](phases/PHASE_4_DESIGN.md)

### Phase 5: Continuous Strategy Lifecycle

Objectives:

- Continue proposing and testing new strategies.
- Route every strategy through the same Research, Backtest, Paper, and Live
  gates.
- Promote improving strategies and reduce, pause, or retire deteriorating
  strategies.
- Preserve failures and all state transitions.

Design: [`PHASE_5_DESIGN.md`](phases/PHASE_5_DESIGN.md)

---

## 6. Implementation Dependencies

```text
Data Source
    ↓
Phase 1: AI research and backtesting
    ↓
Phase 2: Platform comparison and Paper Trading
    ↓
Phase 3: User-approved Controlled Live
    ↓
Phase 5: Continuous lifecycle and scaling
```

Phase 4 runs alongside Phase 1 onward. The required risk controls must be
complete before each downstream gate.

---

## 7. Platform Selection Principles

The core question is:

> At the time a strategy must decide, which platform delivers the required
> market data more quickly and accurately and routes orders to CME more safely?

Price matters, but it is not the sole criterion. Fees and entitlements must be
rechecked before Phase 2.

IBKR documentation describes Level 2 market depth through `reqMktDepth`, but it
does not provide a Production guarantee for:

- CME-event-to-callback latency
- Exchange timestamps on every callback
- Exchange sequence
- Snapshot completion markers
- Consistent usable depth for every strategy

These properties must be measured in Phase 2.

Both candidates must be compared using the same active contract, host
conditions, time window, and synchronized clock. Compare:

- Callback delay and jitter
- BBO and L1-L10 price/size differences
- Missing, stale, reset, and recovery behavior
- Feature and signal differences
- Paper submit, acknowledgement, cancel, and recovery behavior
- Fixed costs, transaction costs, and operating complexity

After selection, only one platform may hold Live order credentials.

---

## 8. Live Approval

A strategy that passes backtesting does not enter Paper Trading automatically.
Paper entry requires a separately authorized promotion decision, and explicit
user approval remains mandatory before real capital is used.

The approval package must include:

- Strategy name, version, and hypothesis
- Features, signal interval, entry policy, and applicable regimes
- Backtest period and out-of-sample results
- Paper start/end dates and active trading days
- Signal and order counts
- Order types, average holding time, and exit reasons
- Take-profit-first, stop-first, roll-exit, emergency-exit, and censored counts
- Time-to-hit and capital-occupancy distributions
- Gross and fully loaded net PnL
- Maximum drawdown and consecutive losses
- Slippage, partial fills, rejects, and operational incidents
- Selected platform and selection rationale
- Proposed one-contract take-profit, stop-loss, potential profit, planned loss,
  and reward-to-risk
- Live stop conditions

Production orders are prohibited before approval.

---

## 9. Governing Risk Rules

1. AI and research daemons cannot place Paper or Live orders, change risk
   limits, unseal holdout data, promote candidates, or increase capital.
2. Exactly one automated Production broker writer may exist. Manual emergency
   actions must be detected and reconciled before automation continues.
3. The selected broker is the operational source of truth for orders, fills,
   and intraday positions.
4. Unresolved orders or position mismatches block new exposure. An ambiguous
   submit result must be marked `UNKNOWN`, reconciled against broker state,
   and never blindly retried.
5. Every exposure-increasing Paper and Live entry must define its take-profit
   target and stop-loss and use a verified broker-managed OCO bracket.
6. A strategy must define direction, entry policy, take-profit price,
   stop-trigger price, stop execution policy, and terminal roll/expiry policy.
   A stop trigger is not a guaranteed fill price.
7. No alpha-imposed maximum holding period is required. Time-to-hit, open-risk
   duration, capital occupancy, and weekend exposure must still be measured,
   and risk or delivery controls may force an earlier exit.
8. Initial Live allocation is one 6E contract.
9. Daily, strategy/project cumulative, and drawdown limits are mandatory.
10. Stale, incomplete, or unavailable market data blocks new entries.
11. Physical delivery is not intended; positions and orders must be closed
   before expiry deadlines.
12. A selected-platform failure does not trigger automatic failover.
13. Capital increases, risk relaxation, normal reactivation, and activation of
    a new Live strategy require user approval.

These are non-numeric system invariants. `VALIDATION.md` exclusively owns the
numeric thresholds. Phase 4 owns risk semantics and enforcement; Phase 3
applies those controls during Controlled Live and records the resulting
evidence. Child designs may specialize these rules but cannot weaken them.

---

## 10. Strategy Lifecycle

```text
RESEARCH
    ↓ validation pass
BACKTEST_PASSED
    ↓ separately authorized Paper promotion
PAPER
    ↓ evidence package and user approval
LIVE_APPROVED
    ↓ one-contract launch
CANARY
    ↓ sustained Live evidence
ACTIVE
    ↓ approved scaling
SCALED
```

Deterioration path:

```text
CANARY | ACTIVE | SCALED
    ↓
REDUCED
    ↓
PAUSED
    ↓
REVALIDATING
    ├─ return to PAPER or CANARY
    └─ RETIRED
```

Safety breaches may automatically block new orders. Normal Live activation,
reactivation, and capital increases require user approval.

---

## 11. Technical Direction

- Python 3.12+
- Parquet with Polars/PyArrow
- PostgreSQL for metadata and trading state
- AWS S3 for historical storage
- A deterministic custom event-driven backtester with a content-addressed
  date/contract cache and one shared chronological replay pass
- Reproducible Docker environments
- pytest, golden fixtures, and property tests

Technology is an implementation means, not a project objective. Do not add
distributed infrastructure before measured requirements justify it.

---

## 12. Handling Unknowns

1. Do not guess behavior before the responsible phase.
2. Record the source and verification date for documented behavior.
3. Measure behavior that documentation does not guarantee.
4. Record findings in the responsible phase design.
5. Amend `DESIGN.md` only when a finding changes project direction.

Examples:

- IBKR callback latency: Phase 2
- Rithmic depth semantics: Phase 2
- Order races and reconnect behavior: Phases 2 and 3
- Risk thresholds: `VALIDATION.md` and Phase 4
- Licensing and automation disclosures: before the relevant paid or Live use

---

## 13. Stop or Redesign Conditions

Consider stopping or redesigning the project without weakening standards when:

- Every preregistered hypothesis family fails under realistic costs.
- Reliable Historical MBP-10 data cannot be obtained.
- Neither Live candidate provides sufficient data quality or order safety.
- Platform and transaction costs structurally exceed the verified edge.
- Safe protection, reconciliation, or emergency exit cannot be implemented.
- Required capital is unavailable.
- Live approval would require weakening validation standards.

A third platform or another product is not an automatic fallback. Either
requires a separate user decision and design amendment.

---

## 14. Official References

### IBKR

- TWS API:
  https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/
- Market depth:
  https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/market-depth-l-2/introduction
- Request market depth:
  https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/market-depth-l-2/request-market-depth
- Receive market depth:
  https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/market-depth-l-2/receive-market-depth
- Market-data subscriptions:
  https://www.interactivebrokers.com/docs/general/market-data-subscriptions/introduction

### Rithmic and AMP

- Rithmic API suite: https://www.rithmic.com/products/api-suite
- AMP Rithmic API: https://www.ampfutures.com/trading-platform/rithmic-r-api
