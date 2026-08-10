# Conservative Phase 1A Screening Policy

- Document version: 1.3.0
- Status: `FROZEN_SCREENING_POLICY`
- Scope: MBP-10 Phase 1A screening only
- Campaign: `phase1a_conservative_screening_v1`

## Purpose and authority ceiling

This policy defines a deliberately conservative first screen for 6E MBP-10
research. Its maximum positive conclusion is `SCREENING_SURVIVOR`. A survivor
is only a candidate worth carrying into a separately registered backtest and
validation campaign. It is not `PASS_BACKTEST`, does not permit Paper trading,
and grants no Live authority.

The only terminal screening labels are:

```text
SCREENING_REJECT
SCREENING_SURVIVOR
```

`PASS_BACKTEST`, `ENTER_PAPER`, `PAPER`, and all Live labels are forbidden in
this campaign. The existing validation and promotion policy is not weakened or
satisfied by this screen.

## Versioned configuration set

The policy is machine-readable in four files:

| Concern | Configuration |
|---|---|
| Campaign authority, data exclusions, and contract choice | `configs/campaigns/phase1a_conservative_screening_v1.toml` |
| Costs and fully loaded allocation | `configs/costs/phase1a_conservative_cost_v1.toml` |
| Entry, barrier execution, latency, and stress | `configs/execution/phase1a_conservative_execution_v1.toml` |
| Complete 22 by 22 barrier surface and stability | `configs/research/phase1a_barrier_grid_v1.toml` |

Every run records the four configuration hashes, source and split manifest
hashes, the frozen conservative fixed-cost assumptions, immutable code version,
dependency-lock hash, runtime environment, seed, and every signal/model
parameter. A policy change after results exist requires new identifiers.

## Raw data and the six failed dates

Raw parquet and its QC result remain immutable. A derived artifact cannot
relabel a raw `FAIL` as `PASS`, and an exclusion cannot erase the original
failure. The six known structural-failure source dates are excluded in full,
across every contract, session, feature, label, screen, and stress run:

```text
2024-06-30
2024-07-01
2024-07-14
2026-04-19
2026-06-07
2026-06-21
```

This is an entire-source-date exclusion, not permission to clean selected rows.
All other sources still have to satisfy the registered qualification gates.

## Point-in-time active contract

The execution contract is selected once per trading session from 6E outright
symbols present in the immutable source mapping. Selection uses only trade
volume from the previous completed session. Same-session and future information
are forbidden. The contract month is parsed from the raw CME month code and its
one-digit year is disambiguated using the source date. Ties resolve first to the
nearest later symbol-derived contract month and then to ascending instrument
ID. The choice is locked for the current session; there is no intraday roll.

Point-in-time `definition` and `status` are not required for this screening-only
campaign and their absence is part of its authority ceiling. They remain
mandatory before `PASS_BACKTEST` or Paper eligibility. An unparseable or
ambiguous symbol, a missing previous session, or ambiguous observable market
state excludes the whole session rather than inventing reference state.

Contracts are removed before expiry risk becomes acute: no new entry is allowed
from the first trading session whose trading-date month equals the contract's
expiry month. Any inherited open position has a mandatory terminal exit at the
last valid executable quote before that month starts. Unknown expiry means no
entry.

## Entry and no-entry states

A signal is available only after its right-closed five-minute decision bucket.
The baseline simulated route delay is one second on the receive-time clock.
No event before that eligibility time may fill the order.

Entry is blocked at decision close, during the routing interval, or at entry
eligibility if the book is invalid, stale, reset and not rearmed, locked,
crossed, undefined, missing executable depth, or lacks observable market
activity. When a separate status event is absent, the screen requires a
continuously valid, fresh executable book and records that reference status was
unavailable; it never fabricates an ACTIVE status.
A quote older than one second is stale. After a reset, one complete valid second
is required before entry may rearm. These rules block entry; they do not allow
an open position to lose its protective exit.

At the source-date batch boundary, a structurally valid file whose hash-bound
QC evidence proves that every row is a snapshot at the exact source start has
no proven complete observed one-second bucket. The date remains in the frozen
source-date proxy split but is recorded as `RECORDED_NO_ENTRY` with reason
`NO_PROVEN_COMPLETE_OBSERVED_1S_BUCKET`. Its contract-selection and
previous-volume evidence remain attached to the RunSpec and feature manifest.

When the canonical previous-source-only selection proves that the selected
volume-ranked candidate has both zero trade rows and zero trade volume, the
frozen `missing_previous_session_behavior = "NO_ENTRY_ENTIRE_SESSION"` policy
records the date as `RECORDED_NO_ENTRY` with reason
`NO_POSITIVE_PREVIOUS_SOURCE_TRADE_VOLUME`. The selection and previous-volume
documents and their SHA-256 identities remain attached to the RunSpec and
feature manifest. Since the selected candidate is ranked first by prior trade
volume, a zero selected volume proves that every eligible candidate has zero
volume.

Both classifications are deliberately narrow. The planner verifies the raw
source hash, qualified schema, eligible footer mapping, canonical selection,
and previous-volume evidence before returning either reason. A rows/volume
positivity mismatch, arbitrary empty output, or any other feature-build error
still fails closed and cannot be converted to no-entry.

The entry is a one-contract IOC marketable limit at the delayed best ask for a
buy or delayed best bid for a sell, with zero ticks of price-cap expansion. It
cannot walk to a worse level, assume queue priority, partially fill, or retry.
Insufficient size or a price outside the cap is `ENTRY_NOT_FILLED`.
The moderate and severe entry-adversity adjustments remain subject to this
same frozen cap; if the stressed fill price would cross it, that scenario is
recorded as `ENTRY_NOT_FILLED` rather than walking or clipping the fill.

## Buying, target, and loss prices

All distances are anchored to the actual entry fill, never the signal midpoint.
For a long position:

```text
buying price      = actual entry fill
sell target       = entry fill + take-profit ticks
loss trigger      = entry fill - stop-loss ticks
```

For a short position the signs reverse. A take-profit touch is not a fill. The
executable side must trade one full tick through the target; the recorded limit
fill remains at the target price.

A stop trigger and stop fill are separate events and both must be recorded.
After the one-second stop routing delay, the baseline fill is the worse of the
first valid executable price and two ticks adverse to the trigger. A larger
gap is preserved rather than capped. If take-profit and stop are both eligible
at the same timestamp or event key, `STOP_FIRST` wins. Five-minute high/low
ordering is never sufficient.

## Explicit costs and fixed-cost allocation

Every filled and reconciled round trip receives an explicit four-tick ($25 per
6E contract at $6.25 per tick) variable-cost debit. Executable spread and depth
already appear in entry and exit fills, while latency and adverse stop prices
belong to the execution model; those effects are not charged twice.

Fully loaded accounting is mandatory. This screening freezes a deliberately
conservative USD 500 monthly pool and 20 expected filled round trips per month,
which allocates USD 25, or four additional ticks, to every filled round trip.
Together with the four-tick variable debit, the baseline fully loaded cost is
eight ticks and the three-times cost floor is 24 ticks (12 pips). The pool is
USD 150 market data/API, 50 license, 75 platform/routing subscription, 50
broker/account, 50 connectivity/infrastructure, 100 compute/storage, and 25
other recurring costs. These are screening assumptions, not claimed invoices.

Monthly PnL bears the full fixed pool even in a zero-fill month. Per-trade EV
reports the same pool divided by the preregistered expected monthly fills. The
economic take-profit floor is three times the four-tick debit plus allocated
fixed cost, with a ten-tick absolute minimum. A cell under the resulting floor
is still run and recorded, but is rejected.

## Complete barrier surface

Both take-profit and stop-loss axes use 12 through 96 pips in four-pip steps:

```text
pips:  12, 16, 20, ..., 92, 96          (22 values)
ticks: 24, 32, 40, ..., 184, 192        (22 values)
```

Because one pip is two 6E ticks, this is the complete 22 by 22 Cartesian
surface: 484 cells for every candidate, direction, and split. Preselection or
early pruning is prohibited. No-signal, no-fill, censored, cost-floor,
execution-failure, negative, and excluded-source outcomes remain in the trial
ledger. A missing or duplicate cell is a hard run failure.

## Shared chronological outcome architecture

The complete surface is not permission to run 484 independent raw-data
backtests. Raw MBP-10 files must never be scanned once per occurrence, stress
scenario, direction, or cell. A bounded date/contract worker pool first builds
immutable, content-addressed date/contract event partitions under:

```text
data/derived/backtest_event_cache/<cache_version>/
```

Each exact version/date/contract key is built once and records the raw SHA-256,
decoder/schema identity, event-order bounds, row count, byte size, and partition
SHA-256 in the cache manifest. Parallelism ends at independent cache-key
construction and verification; worker count and the one-cache-key-per-worker
scheduling rule are recorded in the RunSpec.

For `phase1a_p5_outcome_replay_v1` and
`phase1a_p1_05_outcome_replay_v1`, both the worker ceiling and maximum number of
in-flight partitions are four. The operator may select one through four
workers. A semantic request index binds each exact cache request to the verified
content-addressed artifact so an ordinary exact rerun reuses it without opening
the raw source again.

The screening runner consumes the verified cache once in strict chronological
order and fans the same event stream out to:

```text
BASELINE | MODERATE_COMBINED | SEVERE_DIAGNOSTIC
    x LONG | SHORT
    x execution contract
    x 484 independent cell-occupancy states
```

Threshold arithmetic may be vectorized, but occupancy may not be shared
between cells: a different exit time changes whether a later signal is skipped.
Independent time-range, occurrence, or cell replays are prohibited. A
scenario, direction, or contract may not create an independent replay shard;
all such states update inside the same single logical chronological pass. Pure
vectorized arithmetic within one ordered event step is allowed.

Source dates advance strictly. Within one source date, the canonical total
order is `(ts_recv_ns, sequence, event_index, contract_key)`. The runner rejects
a duplicate or regression rather than selecting a convenient barrier order.

The contract dimension above is logical occupancy, not the compact summary key.
For the frozen p5 inputs, every signal produces one result identity under every
scenario and barrier cell, for 1,613,172 detail rows (`1,111 x 3 x 484`). The
frozen p1_05 inputs require 1,369,236 (`943 x 3 x 484`). Those rows retain
`signal_id`, direction, and futures contract. The immutable Discovery
occurrence addressed by `signal_id` remains the source of every original
research variable, avoiding 1,452 duplicate variable objects per signal while
preserving exact reconstruction. Each candidate's final surface has 2,904
summaries (`3 x 2 x 484`) keyed by scenario, direction, take-profit, and
stop-loss and aggregated across the seven contracts.

The 20-active-session first-touch clock is distinct from portfolio
continuation. An unresolved observation freezes as `CENSORED`; its cell remains
occupied, continues to log skipped signals, and follows later cached events to
take-profit, stop, or terminal roll/expiry exit. The eventual portfolio exit
does not rewrite the first-touch label.

## Reproducible run identity

Phase 1A does not permit an unregistered exploratory calculation. Before a
feature batch, AI slice, fixed query, barrier surface, or stress replay runs,
its complete input state is canonicalized into an immutable `RunSpec`. This
includes all source and derived hashes, full policies and thresholds, the
12--96 pip grid, cost and execution scenarios, calendar/split identities,
random seed, runtime environment, and exact code/config snapshot. An exact
successful fingerprint is not rerun; the new attempt is retained as
`SKIPPED_DUPLICATE` and points to the prior result.

All eleven fixed first-pass queries are recorded even when support is zero.
Every five-source-date result artifact, per-query exposure, forward unresolved
case, and counterexample remains linked to its own governed run identity in
PostgreSQL. The pattern table may summarize accumulated slices, but it cannot
replace or delete those immutable slice records.

Discovery slices are source-time ordered. Before slice `N > 0` starts any new
run attempt, PostgreSQL must verify the exact completed AI exposure, eleven
query exposures, successful artifact links, and eleven pattern observations
for slice `N - 1`; counts without matching identities are insufficient.

Registration-only recovery after a code revision is governed separately from
research calculation. A successful campaign-level `VALIDATION` RunSpec owns an
immutable recovery manifest below `data/derived`, and every recovered QUERY or
pattern points back to that exact control run and manifest. The original AI
artifact and analysis-code identity never change, and the recovery contract
forbids feature construction or AI analysis recomputation.

A terminal feature-only failure is not treated as a partial research result.
It may start a new governed attempt only when every matching RunSpec is
`FEATURE_BUILD`, every attempt is `FAILED`, all result/reuse/trade-ledger links
are null, and no AI exposure or pattern observation exists. The failed history
is retained. Any active, successful, artifact-linked, or mixed prefix still
fails closed.

Outcome replay checkpoints are published only after a complete source-date
barrier. Each checkpoint is content addressed and binds the RunSpec, cache
manifest, last consumed partition, complete scenario/direction/contract/cell
state, prior checkpoint, and producing attempt. Resume continues only that same
active attempt and must reproduce the uninterrupted canonical result bytes;
attempt and checkpoint history remains append-preserved, and a terminal failed
attempt cannot be reopened or continued after any input/hash drift.

The first governed outcome candidate,
`p5_01_range_expansion_flow_continuation`, completed its 485-date resumed replay
and both directional surfaces were screening-rejected. A rejection does not
remove the predecessor gate. The required independent full uninterrupted replay
matched all 485 resumed daily shards, checkpoints, summaries, and final result
bytes. Its content-addressed proof SHA-256 is
`b878bdfcd65a481f0710a5be5af5e4c77392260392c164ccd86db1cde6f1d309`;
successful `VALIDATION` RunSpec `1303` and attempt `1302` own `PASSED`
append-only equivalence-audit row `1`. Only that row authorized
`p1_05_unconfirmed_move_reversal` to reserve an outcome attempt. Cache
partitions were reusable by verified content hash, while candidate RunSpecs,
attempts, checkpoints, and result artifacts remained separate.

The p1_05 input is frozen at 943 signals (446 `LONG`, 497 `SHORT`), 216 signal
dates, seven contracts, and 478 replay partitions from 2022-01-07 through
2023-08-31. Its plan and immutable cache were prepared, and the final run reused
all 478 verified cache partitions. The governed replay succeeded as manifest
`4`, RunSpec `1306`, and attempt `1305`; it processed 854,765,427 ordered events,
produced all 1,369,236 detail rows and 2,904 summaries, and completed checkpoint
sequence 478. Its result SHA-256 is
`0bd8f465bb3bb47a7f9f72662f905a19a416802a5d8ebff23cdeefd66fcc10ce`,
and its final checkpoint SHA-256 is
`ede238cf6c45287294cc1dce2927f63dd7d2d8a78dda76f5ff59ec1c102a96de`.
Independent verification matched all 478 detail shards and checkpoints, all
1,369,236 decoded rows, all 2,904 summaries, and the frozen selector. LONG has
0 baseline-positive, 85 moderate-positive, 0 joint-positive, and 0 stable cells.
SHORT has 5 baseline-positive, 383 moderate-positive, and 5 joint-positive
cells, but the joint region splits into components of sizes `[2, 2, 1]` and has
0 stable cells. The five SHORT joint cells are 52/96, 56/96, 92/88, 92/96, and
96/88 TP/SL pips. No scenario/direction has a calendar-month-loaded positive
cell.

Both directions are therefore `SCREENING_REJECT`. Their selected TP/SL values
are null, `positive_region_size = 0`, and both record the same three reasons:
`JOINT_POSITIVE_REGION_NOT_SINGLE_CONTIGUOUS_COMPONENT`,
`NO_INTERIOR_7_OF_9_STABLE_CELL`, and `NO_STABLE_REGION_MEDOID`.

## Adjacent stability

A survivor cannot be an isolated optimum. For an interior selected cell, its
3 by 3 Chebyshev neighborhood on grid indexes must satisfy the rule under both
baseline and moderate combined stress:

```text
positive fully loaded net-EV cells:       at least 7 of 9
neighbor median EV / selected EV:         at least 0.50
selected EV / positive-neighbor median:   at most 2.00
```

The positive cells must form a contiguous region. Boundary cells cannot
survive this screen because they lack a complete neighborhood; expanding the
surface requires a new preregistered grid. Selection uses the region medoid,
falling back to the least-stop-loss cell within ten percent of the region's
median EV. Selecting the single best cell is prohibited.

## Stress scenarios

Every one of the 484 cells is recorded under all three scenarios:

| Assumption | Baseline | Moderate combined | Severe diagnostic |
|---|---:|---:|---:|
| Round-trip variable debit | 4 ticks | 5 ticks | 6 ticks |
| Fixed-cost pool | 1.00x | 1.25x | 1.50x |
| Routing delay | 1.0 s | 1.5 s | 2.0 s |
| Entry adverse adjustment | 0 ticks | 1 tick | 2 ticks |
| TP trade-through | 1 tick | 1 tick | 2 ticks |
| Other market-exit adjustment | 0 ticks | 1 tick | 2 ticks |
| Minimum stop adversity | 2 ticks | 4 ticks | 6 ticks |

Baseline fully loaded net EV must be positive. Moderate combined fully loaded
net PnL must be positive with profit factor at least 1.05, and the adjacent
stability rule must pass. Severe results are mandatory diagnostics but are not
a promotion gate. Even after all of these conditions pass, the result is only
`SCREENING_SURVIVOR`.

## What this policy does not claim

The completed p5 screen supports only `SCREENING_REJECT` for each direction; it
does not establish a tradable strategy or provide independent walk-forward or
holdout evidence. The p5 uninterrupted-versus-resumed equivalence audit and the
p1_05 economic replay have completed, and the verified frozen decision rejects
both p1_05 directions. No p1_05 TP/SL pair was selected, so this policy provides
no Production Buying Price, Sell Price, or Loss Price triplet. The runner must
fail closed when a required input, cache binding, audit row, lineage link,
candidate-specific detail record, or any of the 2,904 aggregate summaries is
absent. A later backtest must add point-in-time definition/status, actual cost
evidence, its own registered campaign, numeric validation gates, and explicit
authorization; neither completed screen has `PASS_BACKTEST`, Paper, or Live
authority.
