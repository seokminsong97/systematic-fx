# Validation and Promotion Criteria

- Document version: 1.3.0-draft
- Revised: 2026-08-09
- Status: `DRAFT`
- Parent document: [`DESIGN.md`](DESIGN.md)
- Research scope: [`RESEARCH_PLAN.md`](RESEARCH_PLAN.md)
- Applies to: Research, Backtest, Paper, Controlled Live, and promotion

---

## 1. Purpose and Authority

This document is the exclusive owner of numeric acceptance thresholds. It
defines when evidence is sufficient to:

```text
REJECT
INCONCLUSIVE
PASS_BACKTEST
ENTER_PAPER
REQUEST_LIVE_APPROVAL
CONTINUE_CANARY
PROMOTE
REDUCE
PAUSE
RETIRE
```

AI may execute this policy but cannot modify it, choose favorable periods,
remove trials, waive failed criteria, or reveal sealed-holdout information
before a strategy artifact is frozen.

Passing is necessary but not sufficient for Live trading. Explicit user
approval remains mandatory.

---

## 2. Core Definitions

### Candidate

A fully specified strategy artifact with immutable feature, signal, direction,
entry, take-profit, stop, execution, cost, contract, and terminal-exit policies.

### Variant

Any change to direction, feature set, lookback, threshold, model, model seed,
entry rule, barrier rule, execution assumption, applicable regime, or terminal
policy creates a distinct variant and trial.

### Filled trade

One entry intent that receives at least one fill and is reconciled through
take-profit, stop, terminal exit, or an explicitly reported open/censored state.
Partial fills belong to the same trade intent.

### Active entry day

An eligible trading day on which the frozen strategy could evaluate signals
and its required features and execution book were valid during at least 95% of
its declared entry window. A day is not active merely because an inherited open
position remains protected.

### Net economics

All net results include the registered spread and fill-price effect, slippage,
commissions, exchange/regulatory/routing fees, and applicable fixed operating
cost allocation. Costs already represented in fill prices are not added again.

### First-touch observation

Barrier order determined from event-ordered, executable-side MBP-10 and the
registered fill model. Five-minute high/low ordering is not evidence.

### Portfolio continuation

The actual cell-specific position state after entry. A first-touch observation
may freeze as `CENSORED` after 20 active sessions while portfolio continuation
remains occupied until take-profit, stop, or mandatory terminal exit. The later
portfolio exit cannot retroactively change the first-touch label.

---

## 3. Data Qualification

Paper eligibility requires at least:

```text
eligible active days:             740
quarterly roll cycles represented: 12
sealed-holdout decision days:     120
sealed outcome-tail days:          20
```

The 740-day minimum includes the outcome tail described below. Additional data
is distributed between Discovery and walk-forward validation by the fixed rule
in Section 4; it cannot be assigned after results are known.

Every included raw file must pass:

- Checksum and container/frame validation
- Dataset, schema, symbol, and time-range validation
- Initial book snapshot/clear requirements
- Tick-size and contract-metadata validation
- Invalid-price sentinel masking in derived features
- Event ordering, duplicate, and sequence-quality checks
- Book validity and missing-interval classification

At least 98% of scheduled days in the selected historical range must qualify as
eligible. Missing or invalid intervals remain visible in reports, block new
entries, and cannot be forward-filled across a session break or book reset.

The local seven-file fixture is a pipeline pilot only. No result from it can
pass Backtest or enter Paper.

---

## 4. Deterministic Multi-Period Split

The split is generated from eligible active days before any strategy result is
computed. AI and researchers may not choose dates based on performance.

Reserve the latest 160 eligible days ending at the selected roll cutoff:

```text
Holdout embargo:           20 active days
Sealed holdout decisions:  120 active days
Sealed outcome tail:       20 active days ending at a known roll cutoff
```

Let `P` be all eligible days before that 160-day reservation. With the 740-day
minimum, `P >= 580`. Allocate:

```text
extra = P - 580
Discovery days = 220 + floor(0.40 * extra)
Walk-forward days = every remaining day in P
Walk-forward folds = 5 consecutive, near-equal blocks
Minimum fold length = 72 active days
```

Assign any fold-division remainder one day at a time from the oldest fold. This
keeps approximately 40% of additional history in Discovery and 60% in
multi-period validation instead of concentrating all independent evidence near
the end.

The splitter selects the latest verified delivery-avoidance/roll cutoff for
which the complete structure fits, then works backward from that cutoff. Every
boundary is immutable for the campaign.

### Walk-forward procedure

The signal definition, feature set, hyperparameters, barrier-selection rule,
execution model, and cost model are frozen before Fold 1.

For each fold:

1. Fit only model coefficients and train-defined transforms that the frozen
   artifact explicitly permits to refit.
2. Use all chronologically prior permitted data.
3. Exclude the most recent 20 active days from fitting when their barrier labels
   are not yet mature.
4. Apply the frozen artifact to the complete next fold without adjustment.
5. Preserve every signal, no-fill, filled trade, skipped signal, and outcome.

Validation fold reports remain hidden from AI until all five folds have run.
If a result is inspected and then used to modify a strategy, every inspected
fold becomes Discovery for that descendant. The descendant needs new later
validation periods and cannot claim the same five folds as independent.

### Sealed holdout

- No holdout feature, label, event window, summary, distribution, or result may
  be shown to AI before the artifact and finalist set are frozen.
- The 20-day embargo cannot train, select, or calibrate a finalist.
- No new holdout signals are allowed in the outcome tail.
- The outcome tail ends at the preselected roll cutoff. Any position still open
  at that cutoff receives the artifact's mandatory terminal exit.
- The 20-session first-touch observation rule may label an observation
  `CENSORED`, but the portfolio simulation continues it to an actual take-profit,
  stop, or terminal exit.
- Run the sealed holdout once for each frozen finalist set.
- A strategy changed after holdout inspection is a new artifact and requires a
  fresh prospective holdout.

---

## 5. Leakage and Reproducibility Gates

Any failure in this section is `FAIL` and blocks all economic interpretation.

- Features at decision time use only events available by that time.
- Contract selection uses only contemporaneously available information.
- Train-defined thresholds and transforms are unchanged during each test fold.
- Split boundaries and purge windows match Section 4.
- The sealed holdout was not queried before artifact freeze.
- Every evaluated variant and barrier cell is present in the trial registry.
- Rerunning from source checksums and the recorded code/container produces
  identical signals and integer-tick order prices.
- Event-level barrier order is deterministic.
- Costs and invalid/no-fill outcomes cannot be disabled by a strategy.

### Shared-replay implementation gate

Any failure below blocks outcome interpretation, including a screening-only
survivor decision:

- Every raw MBP source used by the outcome engine is bound by SHA-256 to one
  complete cache-build manifest.
- Cache partitions are immutable, content-addressed, date/contract scoped, and
  stored below `data/derived`; filename, byte size, schema, row count, event
  bounds, source hash, and actual content hash agree.
- Raw-source and cache Parquet hashing and decoding use the same held file
  descriptor. Request/report identity uses the same typed date, contract,
  source SHA, event offset, and portable `data/`-relative source URI. Tests must
  reject inode replacement, symlink/path-component swaps, and same-size content
  changes before those bytes can enter replay or final evidence.
- Instrumented source-open counts prove that raw MBP is not scanned per signal
  occurrence, scenario, direction, or barrier cell. Each required
  version/date/contract cache key is built at most once and reused by hash.
- The chronological runner reads each required verified cache partition once
  and applies events to every registered state. Source dates are strictly
  increasing, and the exact within-date key is
  `(ts_recv_ns, sequence, event_index, contract_key)`; duplicates and
  regressions are hard failures.
- A contract's nominal last pre-expiry calendar partition is never accepted as
  terminal merely because it is last. Complete cache reports are reverse-
  scanned to the latest partition with a valid executable quote and coherent
  last-valid event/time metadata. A trailing zero-valid partition must fall
  back, no valid partition must fail closed, and invalid-only partitions after
  the resolved terminal must be verified without entering the economic stream.
  The policy and full selection hash must agree across the RunSpec, checkpoint
  input lineage, final result, and the cache-manifest report facts.
- For every scenario, direction, and futures contract there are exactly 484
  logical barrier-cell occupancy states. The p5 detail ledger contains exactly
  1,613,172 signal/scenario/cell rows (`1,111 x 3 x 484`); the p1_05 ledger must
  contain exactly 1,369,236 (`943 x 3 x 484`). Both retain direction, contract,
  occurrence lineage, and lossless access to every original signal variable.
  Each compact aggregate contains exactly 2,904 scenario/direction/cell
  summaries (`3 x 2 x 484`). Missing, duplicate, preselected, or silently
  pruned states, detail rows, summaries, or variable lineage are hard failures.
- Each cell preserves its own occupancy, skipped signals, 20-session
  first-touch clock, immutable censor label, and eventual portfolio exit.
- A reviewed golden fixture produces identical entries, fill prices, barrier
  order, censor labels, portfolio exits, skipped-signal counts, and costs under
  the reference replay and shared chronological replay.
- The configured cache ceiling is four workers and four in-flight partitions,
  with one cache key per worker. The actual operator-selected count from one
  through four is recorded. Changing that count cannot change canonical cache
  bytes. Economic replay has exactly one logical chronological pass and no
  scenario, direction, contract, occurrence, time-range, or cell replay shards.
- Checkpoint artifacts are content addressed and lineage bound. Clean runs and
  injected stops after every supported date boundary resume to byte-identical
  final artifacts without duplicated or omitted events. For the real p5
  predecessor, an independent full uninterrupted replay must also reproduce
  every persisted daily shard, checkpoint, compact summary, and final canonical
  document from the completed resumed run byte for byte.
- Resume and final verification process cumulative detail evidence one daily
  shard at a time. A mode that omits record decoding must still stream-verify
  every referenced shard SHA-256; filename, size, and read-only mode alone are
  insufficient evidence. An instrumentation test rejects retention of either
  complete candidate ledger in memory.
- No later-date state or checkpoint commits before the complete prior-date
  barrier; independent time-range, per-occurrence, and per-cell replay are
  prohibited.
- `p1_05_unconfirmed_move_reversal` cannot reserve an outcome attempt until the
  complete `p5_01_range_expansion_flow_continuation` run and its independent
  uninterrupted-versus-resumed audit have succeeded. The proof must be an
  immutable, content-addressed artifact owned by a successful `VALIDATION`
  attempt and a `PASSED` PostgreSQL equivalence-audit row. The p1_05 RunSpec,
  checkpoints, and result must bind that audit row and the predecessor's exact
  replay-manifest, run-fingerprint, result, input-lineage, cell-summary,
  detail-shard-manifest, and final-checkpoint hashes.

Required automated tests include:

```text
future-row mutation leaves earlier features unchanged
label mutation leaves features and signals unchanged
late-event insertion does not rewrite a closed decision bucket
split-boundary mutation does not leak into prior fit state
same-seed rerun is identical
5-minute dual-touch order matches event-level first execution
stop trigger and stop fill remain distinct
raw-source open counts are independent of occurrence and cell counts
single-worker and four-worker cache manifests are identical
reference and shared replay produce identical golden outcomes
20-session censor does not close or relabel portfolio continuation
checkpoint at each supported date boundary resumes byte-identically
full uninterrupted p5 replay reproduces the completed resumed artifact bytes
terminal partition with zero valid quotes falls back to the prior executable partition
contract with no pre-expiry executable quote fails closed
terminal-resolution policy/result tampering breaks RunSpec or checkpoint lineage
missing or duplicate scenario/direction/contract/cell state fails closed
p1_05 reservation fails before the governed p5_01 predecessor completes
```

The real-data p5 cache, 485-date replay, 1,613,172 details, and 2,904 summaries
have completed; both directions were screening-rejected. The separate full
uninterrupted-versus-resumed comparison also passed across all 485 checkpoints
and is registered as equivalence-audit row `1` under successful `VALIDATION`
RunSpec `1303` and attempt `1302`. Its audit artifact SHA-256 is
`b878bdfcd65a481f0710a5be5af5e4c77392260392c164ccd86db1cde6f1d309`.

That registered proof authorized p1_05 RunSpec `1306` and attempt `1305`. The
real-data p1_05 replay then completed all 478 partitions, processed 854,765,427
ordered events, emitted all 1,369,236 details and 2,904 summaries, and committed
result SHA-256
`0bd8f465bb3bb47a7f9f72662f905a19a416802a5d8ebff23cdeefd66fcc10ce`
with final-checkpoint SHA-256
`ede238cf6c45287294cc1dce2927f63dd7d2d8a78dda76f5ff59ec1c102a96de`.
Independent verification passed for all 478 detail shards and checkpoint files,
all 1,369,236 decoded detail rows, the full checkpoint hash chain, all 2,904 DB
and artifact summaries, and exact selector/decision reproduction. LONG has no
baseline-positive or joint-positive cell. SHORT has five joint-positive cells,
but their component sizes are `[2, 2, 1]`; both directions have zero stable
cells and zero calendar-month-loaded positive cells. The registered decisions
therefore reject both directions with null selected TP/SL and
`positive_region_size = 0`. This bounded screen does not satisfy the data
qualification, walk-forward, stress-validation, or sealed-holdout requirements
for `PASS_BACKTEST`; it provides no Production Buying/Sell/Loss triplet and no
Paper promotion evidence.

---

## 6. Minimum Statistical Evidence

### Walk-forward validation

```text
filled trades, aggregate:          300
active entry days, aggregate:      150
filled trades per fold:             40
active entry days per fold:         20
execution contracts represented:     5
```

### Sealed holdout

```text
filled trades:                      80
active entry days:                  40
execution contracts represented:     2
```

If every other gate passes but a count is below the minimum, the result is
`INCONCLUSIVE`, not `PASS` or an economic `FAIL`. Continue the same frozen
artifact prospectively; do not loosen the threshold.

Long and short variants qualify separately. Counts cannot be pooled across two
different direction policies to rescue either one.

---

## 7. Walk-Forward Economic Gates

All metrics use fully loaded net results unless explicitly labeled marginal.
A candidate must satisfy every aggregate condition:

```text
aggregate net PnL:                         > 0
aggregate profit factor:                  >= 1.20
aggregate net profit / max drawdown:      >= 1.50
one-sided 95% bootstrap lower bound EV:    > 0
positive validation folds:                >= 4 of 5
folds with profit factor below 0.75:        0
```

The magnitude of the worst losing fold cannot exceed 1.5 times the median net
profit of positive folds. If all five folds are positive, this condition is
automatically satisfied.

Use stationary block bootstrap on daily strategy equity changes. Select mean
block length from the registered dependence diagnostic, bounded to 5-20 active
days, and run at least 10,000 bootstrap replicates with a registered seed.

The purpose of the 4-of-5 rule is not to permit cherry-picking. All five folds
remain in aggregate economics, uncertainty, drawdown, and multiplicity tests.
A strategy that wins in one period and loses its edge elsewhere cannot pass.

---

## 8. Sealed-Holdout Economic Gates

A finalist must satisfy:

```text
fully loaded net PnL:                      > 0
profit factor:                            >= 1.15
net profit / max drawdown:                >= 1.00
one-sided 90% bootstrap lower bound EV:    > 0
first 60-day half net PnL:                 > 0
second 60-day half net PnL:                > 0
```

If a 60-day half has too few fills to calculate stable economics but aggregate
counts pass, the result is `INCONCLUSIVE`. It does not become a pass by pooling
the halves.

The realized holdout maximum drawdown must not exceed 1.25 times the
pre-registered 95th-percentile drawdown envelope estimated from Discovery and
walk-forward daily returns.

---

## 9. Multiple-Testing Control

All parent hypotheses, descendants, directions, thresholds, seeds, models,
barrier cells, and failed runs belong to the campaign multiplicity ledger.

### Discovery and validation

- Compute one-sided stationary-bootstrap p-values for net EV.
- Apply Benjamini-Hochberg false-discovery-rate control at `q = 0.05` across all
  variants evaluated in the campaign.
- A finalist must retain adjusted significance and pass Sections 7 and 10-12.

### Sealed holdout

- No more than ten finalists may enter one sealed-holdout campaign.
- Apply Holm-Bonferroni family-wise error control at `alpha = 0.05` to the
  finalist net-EV tests.
- Only finalists that retain Holm-adjusted significance may pass.

Variants generated from the same hypothesis are not independent discoveries.
Renaming, regrouping, or selecting a subset cannot reset the trial count.

---

## 10. Barrier and Parameter Stability

A selected take-profit/stop policy cannot be an isolated optimum.

### Absolute barriers

Evaluate the selected cell and its available immediate neighbors on the
registered tick grid. At least:

```text
positive fully loaded net-EV cells: 7 of 9
neighbor median EV / selected EV:   >= 0.50
```

The selected cell's EV cannot exceed twice the median EV of its positive
neighbors. A boundary cell cannot pass unless a new, wider preregistered surface
demonstrates that the positive region continues beyond the old boundary.

### Volatility-normalized barriers and signal thresholds

Re-evaluate take-profit multiplier, stop multiplier, and primary signal
threshold at `0.8x`, `1.0x`, and `1.2x` where valid. At least seven of the nine
barrier combinations at the frozen signal threshold must remain positive, and
both adjacent signal-threshold variants must not reverse aggregate EV below
zero.

These perturbations are validation tests, not opportunities to select a new
best value.

---

## 11. Concentration and Regime Gates

On combined walk-forward validation:

```text
largest fold contribution to positive PnL: <= 50%
largest execution-contract contribution:   <= 35%
top 10 trades / total gross profits:        <= 40%
```

Report results by:

- Validation fold and holdout half
- Execution contract and roll state
- Long or short direction
- Declared session
- Volatility quartile defined from prior training data
- Spread and depth-liquidity quartile
- Weekday and weekend-hold state

A strategy restricted to a preregistered regime is evaluated inside that
regime, but it must emit `NO_TRADE` outside it without using future regime
information. Post hoc regime exclusions create a new variant.

No single event day, contract, or unregistered exclusion may be necessary for a
pass. Removing the best day and the best trade must leave aggregate
walk-forward net PnL above zero.

---

## 12. Execution and Cost Stress Gates

### Baseline

Use executable-side depth after registered routing delay, the verified fee
schedule, conservative passive-fill logic, stop-trigger slippage, and allocated
fixed costs.

### Moderate combined stress

Apply all of the following together:

```text
commissions and variable fees: +25%
entry fill:                     1 tick adverse
take-profit limit:              require 1 tick trade-through before fill
other market exit:              1 tick adverse
stop exit:                      2 additional ticks adverse
routing delay:                  +500 milliseconds
```

The stressed entry price remains subject to the frozen zero-offset IOC limit.
If the full adverse adjustment would cross that limit, record
`ENTRY_NOT_FILLED`; do not walk beyond the cap or clip away the stress.

Moderate-stress gates:

```text
fully loaded net PnL:  > 0
profit factor:         >= 1.05
```

### Severe diagnostic stress

Apply fees `+50%`, entry and market exits `2 ticks` adverse each, require `2`
ticks of trade-through before a take-profit limit fill, add `4` adverse ticks to
stop exits, and add `1 second` of routing delay. Positive PnL is not required,
but severe-stress maximum drawdown cannot exceed twice baseline maximum
drawdown and must remain inside the Phase 4 allocated risk capacity.

Any actual Paper or Live fee, spread, or slippage measurement worse than the
registered stress envelope triggers revalidation with the worse measurement.

---

## 13. First-Touch and Duration Reporting

Holding time is not an alpha pass criterion, but it cannot be hidden.

Report:

```text
take_profit_first
stop_first
terminal_roll_exit
emergency_exit
censored_at_20_sessions
time_to_entry_fill
time_to_take_profit
time_to_stop
total_open_duration
capital_occupancy_percentage
weekend_hold_count
signals_skipped_while_occupied
```

Do not estimate first-touch probability by dropping censored observations. Use
a competing-risk or survival estimator and show the raw counts. Portfolio PnL
uses actual simulated exits, including terminal roll exits.

Once `censored_at_20_sessions` is recorded, a later take-profit, stop, or
terminal portfolio exit is reported in the portfolio fields only. Reports must
not replace the censored first-touch count with that later exit. Conversely,
the portfolio cannot be treated as flat merely because its first-touch label
was censored; occupancy and skipped-signal accounting continue until the
actual exit.

Every holdout entry must be reconciled by the end of its preselected outcome
tail.
An unexplained open state, missing OCO state, or position mismatch is a hard
failure.

---

## 14. Backtest Decision

### `PASS_BACKTEST`

Requires all of Sections 3-13 to pass with a frozen artifact.

### `INCONCLUSIVE`

Use only when data quality passes and point estimates do not violate a hard
economic or safety gate, but minimum counts, outcome maturity, or confidence is
insufficient.

### `FAIL`

Use for leakage, unregistered trials, negative required economics, unstable
parameters, multiplicity failure, stress failure, safety failure, or a sealed
holdout failure.

After any sealed-holdout result is viewed, changing the artifact cannot reuse
that holdout. A failed artifact remains permanently recorded.

---

## 15. Paper Entry and Paper Evidence

`PASS_BACKTEST` automatically permits Paper entry with one simulated contract.

Before a strategy may request Live approval, Paper evidence must include:

```text
active entry days:                  >= 90
filled entries:                     >= 75
reconciled bracket exits:           >= 75
fully loaded estimated net PnL:      > 0
profit factor:                      >= 1.05
critical protection incidents:        0
unreconciled orders or positions:      0
parent fills lacking accepted OCO:     0
```

At least 95% of Paper fills must fall inside the backtest moderate-stress
slippage envelope, and the worst 5% must remain inside the severe diagnostic
envelope. Otherwise revalidate with the observed distribution.

Paper duration, terminal exits, skipped signals, rejects, partial fills, and
all operational incidents remain in the evidence package.

---

## 16. Live Approval Request

A Live approval package may be presented to the user only when:

- Backtest and Paper gates pass.
- The selected platform passes Phase 2 data, order-path, bracket, recovery, and
  reconciliation requirements.
- The exact frozen artifact and one-contract risk proposal are supplied.
- Take-profit, stop trigger, stressed stop fill, planned net target, stressed
  loss, and reward-to-risk are shown in ticks and currency.
- Daily, weekly, cumulative, drawdown, consecutive-loss, and margin limits are
  configured and tested.
- No unresolved critical incident remains.

Approval is not automatic. AI cannot approve or place the first Live order.

---

## 17. Initial Numeric Risk Limits

Let `R` be the artifact's one-contract stressed loss, including entry cost,
stop distance, stressed stop slippage, fees, and gap/halt buffer.

Before Live approval:

```text
R / approved strategy capital:               <= 0.50%
stop new entries at daily loss:               2R or 1.0%, whichever is smaller
stop new entries at weekly loss:              4R or 2.0%, whichever is smaller
pause at strategy drawdown:                   8R or 4.0%, whichever is smaller
disable project entries at project drawdown: 12R or 6.0%, whichever is smaller
pause after consecutive stop-loss exits:      6
maximum initial margin utilization:          35%
initial Live position:                        1 contract
```

Loss and drawdown gates use broker-reconciled realized PnL plus current
executable-side unrealized PnL. For volatility-normalized stops, `R` is recorded
per entry and cumulative limits use normalized realized and open `R`.

These limits do not authorize automatic flattening contrary to Phase 4 outage
and stale-data rules. They block new exposure and invoke the configured safe
transition. Any increase or relaxation requires user approval and a versioned
validation amendment.

---

## 18. Controlled Live and Promotion

Controlled Live remains at one contract. Promotion from canary evidence
requires at least:

```text
active entry days:                    60
statement-reconciled filled trades:   40
statement-reconciled net PnL:          > 0
profit factor:                        >= 1.05
critical safety incidents:              0
unreconciled orders or positions:        0
```

At least 90% of Live fills must remain inside the Paper-observed severe
slippage envelope. A worse distribution, a protection failure, a risk-limit
breach, or drawdown beyond Section 17 pauses promotion and triggers review or
revalidation.

Promotion, scaling, risk relaxation, reactivation, and new strategy activation
still require explicit user approval.

---

## 19. Required Validation Report

Every decision report includes:

- Data manifest, eligible-day inventory, exact split dates, and outcome tail
- Artifact, code, feature, cost, and execution versions
- Event-cache manifest, source bindings, worker bound, and source-open audit
- Checkpoint chain and uninterrupted-versus-resumed equivalence result
- Full trial count and multiplicity adjustments
- Per-fold and aggregate walk-forward results
- Both sealed-holdout half-periods and aggregate result
- Barrier and threshold stability surfaces
- Baseline, moderate, and severe cost/execution results
- First-touch, censoring, duration, and capital-occupancy results
- PnL concentration, regime, contract, and roll breakdowns
- Drawdown and risk-limit simulations
- `PASS`, `FAIL`, or `INCONCLUSIVE` with every failed criterion named

No summary may omit a failed fold, failed variant, censored position, terminal
exit, or operational incident.
