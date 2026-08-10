# Bar Pattern Discovery V1

- Status: Discovery complete; no finalists; walk-forward and holdout remain sealed
- Frozen on: 2026-08-09
- Executed on: 2026-08-09
- Campaign key: `bar_pattern_discovery_v1`
- Qualification: screening only

## 1. Objective and Boundary

This campaign asks whether completed 5-minute, 30-minute, or 1-hour OHLC
patterns have useful forward information when entry is scheduled at the next
signal bar's first trade. It is separate from the Phase 1A MBP-10 signal
queries. The research engine consumes only derived trade OHLCV bars; it does
not consume order-book depth, imbalance, or quote events.

The source container is the local MBP-10 archive because no independent bar
history exists locally. A one-time projection retains only selected-contract
trade prints and publishes neutral 1-second OHLCV. Signal bars are deterministic
resamples of that layer. Discovery and later validation never reopen MBP-10.

The result cannot be promoted to `PASS_BACKTEST`: point-in-time definition and
trading-status inputs remain unavailable, six source dates failed structural
QC, and trade-only bars cannot prove executable bid/ask fills. A survivor is a
candidate for event-level validation, not a production strategy.

## 2. Frozen Data Identity

The raw input identity is the 1,434-file source manifest:

```text
data/derived/manifests/mbp10_source_sha256_v1.jsonl
SHA-256 14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de
2022-01-02 through 2026-07-31
```

The completed derived dataset identity is:

```text
schema                 systematic_fx.trade_bar_dataset_manifest.v1
manifest SHA-256       e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc
build-plan SHA-256     c46323e70e389dd2f7bca4b0e3e42ad86b1a9b7b502834512906e38b4651d0dc
eligible active dates  1,413
daily artifacts        7,065
```

| Timeframe | Rows | Logical bytes |
| --- | ---: | ---: |
| 1 second | 18,655,796 | 1,054,734,846 |
| 1 minute | 1,531,326 | 101,254,485 |
| 5 minutes | 321,262 | 32,263,011 |
| 30 minutes | 54,218 | 15,586,019 |
| 1 hour | 27,138 | 13,477,490 |

Every daily descriptor binds the raw source SHA, point-in-time contract plan,
bar schema, content SHA, byte size, row count, and next-bar-link hash. All
derived files remain below `data/derived/trade_bars/version=trade_bar_v1`.

### Contract and quality policy

- Select an outright 6E contract using only the previous eligible source
  date's positive trade volume.
- Require a contract month strictly later than the current calendar month.
- Break a segment on a contract change or a trade gap of at least 3,600
  seconds.
- Exclude `2024-06-30`, `2024-07-01`, `2024-07-14`, `2026-04-19`,
  `2026-06-07`, and `2026-06-21` before they can affect bars or later volume.
- Use half-open UTC-epoch buckets and integer 6E ticks. One pip is two ticks.

## 3. Candidate Catalog

The catalog contains exactly 216 candidates:

```text
3 signal timeframes x 6 setup lengths x 6 families x 2 directions = 216
```

Signal timeframes are 5 minutes, 30 minutes, and 1 hour. Setup lengths are
`1, 2, 3, 4, 6, 12`. The 240-candidate budget leaves 24 positions permanently
unused in v1; outcome-driven additions are prohibited.

For trigger bar `t`, a candidate may observe the prior ATR20 window ending at
`t-L-1`, setup bars `t-L..t-1`, and completed trigger bar `t`. It may expose
only the open of `t+1`. Calculations use integers or exact rational numbers;
there are no fitted thresholds, quantiles, time filters, regime filters, or
machine-learned parameters.

The six mirrored LONG/SHORT families are:

1. ordered continuation;
2. pullback resumption;
3. exhaustion-rejection reversal;
4. body-engulfing reversal;
5. compressed-range breakout;
6. failed-breakout reversal.

Their exact formulas, gates, economic rationale, direction transform, and
candidate hashes are stored in the frozen candidate definitions, registration
artifact, 216 database trial parameter documents, code snapshot, and every
candidate RunSpec. The TOML freezes the catalog dimensions and shared policy.

## 4. Entry, Exit, and Costs

Entry is scheduled at the first selected-contract trade in the next contiguous
signal bucket. A missing next bucket, contract change, or segment boundary is
`ENTRY_NOT_FILLED`. Each candidate, scenario, TP, and SL cell holds at most one
position; a signal whose entry second is at or before the prior exit second is
`SKIPPED_OCCUPIED`.

The full grid is evaluated without early pruning:

```text
TP ticks = 24..192 step 8
SL ticks = 24..192 step 8
22 x 22 = 484 cells
12..96 pips in 4-pip increments
```

First-touch ordering uses 1-second trade OHLC. If TP and SL are observable in
the same second, SL wins. There is no fixed holding period; unresolved
positions exit conservatively at the contract, quality-segment, or split
boundary.

| Scenario | Entry adversity | TP trade-through | Stop minimum adversity | Terminal adversity | Variable debit | Fixed pool multiplier |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline | 1 tick | 1 tick | 2 ticks | 1 tick | 4 ticks | 1.00 |
| Moderate combined | 2 ticks | 1 tick | 4 ticks | 2 ticks | 5 ticks | 1.25 |
| Severe diagnostic | 3 ticks | 2 ticks | 6 ticks | 3 ticks | 6 ticks | 1.50 |

The base fixed-cost pool is USD 500 per active calendar month and is also
allocated conservatively at 20 expected round trips per month. Selection
requires both per-trade fully loaded economics and calendar-month-loaded
economics; embedded entry/exit adversity is not silently removed.

For a selected LONG cell:

```text
Buying Price = next bar open + scenario entry adversity
Sell Price   = Buying Price + selected TP ticks
Loss Price   = Buying Price - selected SL ticks
```

For a selected SHORT cell:

```text
Sell Price   = next bar open - scenario entry adversity
Buying Price = Sell Price - selected TP ticks
Loss Price   = Sell Price + selected SL ticks
```

These formulas are reported only for candidates that pass all governed gates.

## 5. Frozen Chronological Splits

The split plan is performance-independent and has SHA-256
`5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043`.

- Discovery: 489 active days, `2022-01-03` through `2023-08-02`.
- Discovery decisions: first 469 active days, ending `2023-07-10`.
- Discovery outcome-only tail: final 20 active days.
- Four visible reporting blocks: 118, 117, 117, and 117 decision days.
- Five sealed walk-forward folds: 153, 153, 153, 153, and 152 days; the
  final 20 days of each fold are no-entry outcome tails.
- Holdout embargo: 20 active days.
- Sealed holdout decisions: 120 active days.
- Holdout outcome tail: 20 active days.

Discovery code may open only Discovery partitions. Walk-forward results remain
sealed until all five folds finish; holdout remains sealed until the finalist
set is frozen.

## 6. Discovery Gates

Outcome-free support is required in all four Discovery blocks:

| Timeframe | Raw signals | Signals per block | Distinct days | Maximum median signals/day |
| --- | ---: | ---: | ---: | ---: |
| 5 minutes | 160 | 25 | 40 | 10 |
| 30 minutes | 100 | 15 | 35 | 6 |
| 1 hour | 80 | 12 | 30 | 4 |

All 216 candidates still receive all three 484-cell outcome surfaces. A cell
can enter a positive component only when it has at least 40 fills, at least 8
fills in every block, positive Baseline fully loaded EV, positive Moderate
total and calendar-month-loaded PnL, Moderate PF at least 1.05, at least three
positive blocks, worst-block Moderate EV at least -2 ticks, and no one positive
block contributing more than half of positive-block gross profit.

The component must have at least nine orthogonally connected cells. A selected
cell must be interior, have at least 7 of 9 neighboring cells with positive
Baseline EV and at least 7 of 9 with positive Moderate PnL, and its positive
neighbor median Moderate EV must be at least half its EV. The representative
is the component medoid, with the frozen SL/TP fallback and lexicographic
ranking. At most ten finalists proceed.

## 7. Evidence and Database Lineage

PostgreSQL is the control plane; row-level evidence remains under `data/`.
Before any real outcome is computed, registration freezes:

- the raw and final trade-bar manifest SHAs as distinct identities;
- the eligible calendar and split SHA;
- the complete 216-candidate catalog and every candidate variable;
- feature, signal, entry, barrier, terminal, cost, occupancy, and selection
  policies;
- Git base commit, full dirty-tree code snapshot, dependency lock, Python and
  PostgreSQL runtime identities;
- one canonical RunSpec and attempt per candidate.

Matched contexts and compact 44-threshold first-hit evidence are written as
content-addressed Parquet shards. Those values are sufficient to reconstruct
all 484 Buying/Selling/Loss and actual-exit outcomes without duplicating every
cell in summary JSON. Candidate artifacts retain the full 3 x 484 aggregate
surface, four-block economics, support evidence, decision, and evidence-shard
manifest. Database attempts record successful computation separately from the
candidate trial decision, so an exact rejected screen is reusable rather than
rerun.

## 8. Promotion Rule

Discovery is not a performance claim. Only the frozen top ten may enter all
five walk-forward folds. Only walk-forward survivors may open the sealed
holdout. Even a holdout survivor remains screening evidence until a separately
governed executable-quote/event replay and paper-trading gate succeed.

The completed Discovery produced no finalist, so no walk-forward fold or
holdout partition is authorized to run for this version.

## 9. Completed Discovery Result

The governed run completed successfully on 2026-08-09. All 216 candidate
computations succeeded, but every candidate was rejected by the frozen
screening rules.

| Signal timeframe | Support reject | Economic reject | Finalists |
| --- | ---: | ---: | ---: |
| 5 minutes | 2 | 70 | 0 |
| 30 minutes | 28 | 44 | 0 |
| 1 hour | 72 | 0 | 0 |
| **Total** | **102** | **114** | **0** |

Every support rejection had insufficient raw signals; 99 also failed the
four-block minimum and 81 also failed the distinct-signal-day minimum. Of the
114 candidates that passed support, all 114 lacked a nine-cell contiguous
positive component and all 114 lacked an interior 7-of-9 stable cell. For 113
of them, no cell passed the complete fully-loaded period-stability core at all.

The sole near miss was the 5-minute, six-bar-lookback, F3 exhaustion-rejection
SHORT candidate `bpv1_tf0300_lb06_f3_short`. It had five connected core-eligible
TP/SL cells, below the preregistered minimum component size of nine:

```text
TP/SL pips: 72/92, 76/92, 80/92, 84/88, 84/92
```

This cluster also failed the interior 7-of-9 stability rule. It is retained as
a diagnostic counterexample, not a selected bracket. No price pair in that
cluster is authorized for validation or trading. Across the five cells, fills
ranged from 134 to 155, minimum per-block fills from 26 to 31, Moderate EV from
8.48 to 11.19 ticks, profit factor from 1.114 to 1.143, and
calendar-month-loaded net PnL from USD 268.75 to USD 1,056.25. Positive
single-cell values do not override the failed spatial-stability gate.

The 5-minute contexts were evaluable for 83.7% to 87.9% of decision bars,
depending on lookback, so their 70 economic rejections are the strongest
negative evidence in v1. Thirty-minute context coverage ranged from 27.2% to
51.1%; its 44 support-passing economic rejections are valid, while its 28
support rejections also reflect limited opportunity counts.

### One-hour design limitation

The one-hour result is not evidence that one-hour candle patterns lack an
edge. It is a design-limited, inconclusive branch of v1. The Discovery interval
contained 9,027 one-hour decision bars in 423 gap-derived signal segments. Of
those segments, 394 contained exactly 23 bars and none contained 24 or more.
The frozen context requires an ATR20 window, its preceding close, and `L` setup
bars, so lookback 1 requires 23 bars and lookback 2 already requires 24.

Consequently, only 377 lookback-1 contexts (4.176% of decision bars) were
evaluable; lookbacks 2, 3, 4, 6, and 12 had zero evaluable contexts. The 14
lookback-1 matches were all `ENTRY_NOT_FILLED`. A future version must freeze a
different one-hour context-continuity policy before observing new outcomes,
for example crossing ordinary maintenance closures while still resetting on
contract, quality, and split boundaries. It must use a new campaign/config
identity; v1 is not amended after seeing this result.

### Evidence identity and independent verification

```text
campaign / experiment                 134 / 7446
code commit                           d8adc2cd425ac8dda02fe32a2ef4a6571f15f9a5
code snapshot SHA-256                 b5359052e62deb4333d55489c62964ece7612e7589947b73bd264f76c531f897
bound RunSpecs                        1313..1528 (216)
successful attempts                   1311..1526 (216)
terminal trials                       435..650 (216 REJECTED)
global result artifact                DB 3003
global result SHA-256                 bda2cfef66c6f59469b77d2d4f85f4ccc531a290934c010f99389262bba8cbfa
global artifact identity SHA-256      1ae3a2aade2ee7e8971ac9c0a9fecd6f795fb52f8493837c278f37431b54bd42
evidence manifest SHA-256             58816efcff5a3051195796b35da3e2c3219892a1da633473218850624a5f6a2e
evidence shards                       208 (42 match + 166 replay)
match / replay evidence rows          146,864 / 40,906
terminal result artifacts             216
```

An independent read-only completion validation reopened and rehashed the
global result, evidence manifest, all 208 Parquet shards, and all 216 terminal
JSON artifacts: 426 live files in total. Schema, row counts, complete
`3 x 484` surfaces, RunSpec/trial/attempt lineage, and all aggregate decisions
matched exactly.

Only the 489 Discovery active dates from `2022-01-03` through `2023-08-02`
were opened. Decisions ended on `2023-07-10`; evidence ended at
`2023-08-02 23:58:49 UTC`; walk-forward fold 1 starts on `2023-08-03`. All five
walk-forward folds, embargo, holdout, and holdout outcome tail remain `SEALED`
with no reveal timestamp, and the global replay catalog is empty.

RunSpec `1312`, created by the failed pre-execution 0022 lineage check, has no
trial, attempt, result, or artifact edge. It remains as an immutable audit
trace and is not part of the 216 completed RunSpecs above.

The final result therefore provides no Buying Price, Sell Price, or Loss Price
triplet. It rejects this exact 5-minute/30-minute fixed-family catalog under the
frozen conservative cost and stability rules, while the one-hour branch must
be treated as structurally inconclusive rather than economically rejected.
