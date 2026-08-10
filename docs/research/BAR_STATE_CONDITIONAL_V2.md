# Bar State-Conditional Model V2

- Status: design draft; frozen configuration implemented; Discovery not executed
- Campaign key: `bar_state_conditional_v2`
- Authorized stage: Discovery only
- Parent exposure: `bar_pattern_discovery_v1` (216 candidates, no finalist)
- Qualification: screening only

## 1. Research Decision

V2 does not rerun the six V1 fixed candle formulas on repaired one-hour
segments. That experiment would answer only whether the same narrow catalog
works on one additional clock. Instead, V2 tests the broader proposition that
a small, interpretable model can condition first-touch direction on completed
bar state.

This is not permission for unrestricted pattern mining. V2 contains one model
family, two signal clocks, two frozen feature sets, and three frozen decision
margins. It has exactly twelve candidates. A null result closes this version;
trees, neural networks, new features, or post-result threshold changes require
a new registered campaign.

The engine consumes the existing selected-contract trade-bar dataset. It does
not consume MBP-10 depth or claim executable quote fills. A survivor still
requires later event-level executable-side validation.

## 2. Frozen Candidate Catalog

```text
2 signal clocks x 2 feature sets x 3 confidence margins = 12 candidates
```

- Signal clocks: 5 minutes and 30 minutes.
- Feature sets: `MORPHOLOGY` and `STATE`.
- Margins: `1/20`, `1/10`, and `3/20` (0.05, 0.10, 0.15).
- Direction is model output, not a second candidate dimension.
- Candidate order is timeframe, feature set, then margin.

Canonical keys range from
`bsv2_tf0300_fsmorphology_cm005` through
`bsv2_tf1800_fsstate_cm015`. Each candidate payload contains the complete
feature, model, label, prediction, entry, barrier, cost, and occupancy policy;
the database must bind that payload and its SHA-256 without reconstructing it
from a display name.

## 3. Point-in-Time Features

All features close at signal bar `t`. ATR20 is the exact rational mean of true
range for `t-19..t`, using the close at `t-20`. No feature may read the next
bar, forward-fill a missing bar, cross a contract/quality/source-segment reset,
or fit a transform outside its training interval.

`MORPHOLOGY` contains six values:

```text
ret_1
body_atr
range_atr
upper_wick_atr
lower_wick_atr
close_location
```

`STATE` contains those six plus:

```text
ret_3
ret_6
trend_6_atr
realized_range_6
atr_ratio_5_20
volume_z20
trade_count_z20
buy_imbalance
tod_sin
tod_cos
gap_from_prev_atr
higher_tf_ret_1
```

The configuration freezes every formula and zero/missing-value policy.
Five-minute `higher_tf_ret_1` uses only the most recently completed 30-minute
bar and its prior ATR20. For a 30-minute signal model it is the registered zero
sentinel. `StandardScaler` statistics are fit on training rows only.

## 4. Model and Prediction

V2 permits one model family:

```text
sklearn.linear_model.LogisticRegression
multinomial classes: UP_FIRST, DOWN_FIRST, CENSORED
solver: saga
C: 0.1
l1_ratio: 0.5
class_weight: balanced
max_iter: 5000
tol: 1e-8
random_state: 20260809
```

Under the locked scikit-learn 1.9 API, `l1_ratio=0.5` declares elastic-net
regularization and the deprecated `penalty` argument is deliberately omitted.
`n_jobs` is also omitted because it has no effect and emits a warning in this
version.
The canonical policy separately records the elastic-net declaration and the
exact runtime keyword arguments. A convergence warning or failure is a hard
candidate failure, not a reason to raise `max_iter`, change tolerance, or try a
new seed.

For one fitted row:

```text
score = P(UP_FIRST) - P(DOWN_FIRST)

score >= margin   -> LONG
score <= -margin  -> SHORT
otherwise         -> NO_TRADE
```

An exact tie is `NO_TRADE`. `P(CENSORED)` remains in multinomial normalization
and is never redistributed to the two directional classes.

## 5. Label Contract

The hypothetical entry reference is the first trade of the immediate
chronologically next observed signal bar within the same verified outcome
span. A normal maintenance or missing-bucket gap does not require wall-clock
contiguity; a contract or quality span boundary makes the entry unavailable.
Label distance is symmetric `1.0 x PRIOR_ATR20_TICKS`, snapped to the nearest
eight ticks with half-up rounding and clamped to 24 through 192 ticks. The
label window is exactly 20 active days:

- upper barrier first: `UP_FIRST`;
- lower barrier first: `DOWN_FIRST`;
- neither barrier within 20 active days: `CENSORED`;
- both observable in the same one-second bar:
  `CENSORED_WITH_AMBIGUITY_COUNT`.

Labels are chronological and split-independent. Contract or quality boundaries
censor only an unresolved label: a first touch observed before the later
boundary remains `UP_FIRST` or `DOWN_FIRST`. The frozen ordering is
`UNRESOLVED_AT_BOUNDARY_CENSORED_PRIOR_FIRST_TOUCH_PRESERVED`. An artificial
model-evaluation boundary does not redefine a label. Leakage is prevented by a
fixed 20-active-day training purge, never by including a row merely because
its realized barrier happened to hit early.

This supervised label is distinct from portfolio economics. The portfolio
uses direction-specific `STOP_FIRST` ordering for an ambiguous one-second
touch and receives a mandatory terminal exit at the evaluation split boundary.

## 6. Economic Brackets and Costs

The primary economic surface uses the following TP and SL multipliers:

```text
1/2 | 3/4 | 1 | 3/2 | 2 | 3 | 4
```

Each entry freezes its exact distances as:

```text
distance = clamp(round_half_up_to_8_ticks(multiplier * PRIOR_ATR20_TICKS), 24, 192)
```

The Cartesian surface has `7 x 7 = 49` logical cells. Volatility and clamping
can map multiple multipliers to the same realized distance. Every duplicate is
retained in evidence. If either axis produces fewer than four distinct realized
distances in the candidate's pooled Discovery OOS evidence, the candidate is
rejected rather than credited with a falsely broad stability region.

For a filled LONG:

```text
Buying Price = next-bar first trade + scenario entry adversity
Sell Price   = Buying Price + frozen TP distance
Loss Price   = Buying Price - frozen SL distance
```

For a filled SHORT:

```text
Sell Price   = next-bar first trade - scenario entry adversity
Buying Price = Sell Price - frozen TP distance
Loss Price   = Sell Price + frozen SL distance
```

Baseline, Moderate combined, and Severe diagnostic costs are byte-equivalent
to V1, including USD 500 monthly fixed cost, USD 6.25 tick value, and the
20-round-trip fixed-cost allocation assumption.

Each candidate/scenario/barrier cell holds one net position across both model
directions. A signal while occupied is `SKIPPED_OCCUPIED`; all rows may not be
counted as independent trades.

“Next signal bar” means the immediate chronologically next observed bar of the
same timeframe, contract, and verified outcome span. Ordinary maintenance and
an empty wall-clock bucket do not manufacture a phantom bar and do not prevent
the next observed bar from being the entry bar. A contract or structural-QC
outcome-span boundary does prevent entry and is recorded as not filled.

## 7. Discovery Expanding Schedule

The existing outer split remains immutable. Discovery contains active
ordinals `1..489`; new decisions end at `469`, and `470..489` is the final
outcome tail. V2 adds three inner OOS evaluations:

| Fit | Expanding train | Fixed purge | OOS decisions | Outcome tail |
| --- | --- | --- | --- | --- |
| 1 | `1..98`, 2022-01-03 to 2022-04-28 | `99..118`, 2022-04-29 to 2022-05-22 | `119..215`, 2022-05-23 to 2022-09-12 | `216..235`, 2022-09-13 to 2022-10-05 |
| 2 | `1..215`, through 2022-09-12 | `216..235`, through 2022-10-05 | `236..332`, 2022-10-06 to 2023-01-31 | `333..352`, 2023-02-01 to 2023-02-23 |
| 3 | `1..332`, through 2023-01-31 | `333..352`, through 2023-02-23 | `353..469`, 2023-02-24 to 2023-07-10 | `470..489`, 2023-07-11 to 2023-08-02 |

Only the 311 combined OOS decision days (`97 + 97 + 117`) may select a
candidate. Training fit, in-sample classification, and the three outcome tails
cannot contribute selection economics. All three OOS results should complete
before they are shown to AI or an operator.

After the final Discovery tail, the frozen selection rules may choose at most
four finalists. Feature formulas, feature set, model arguments, margin, bracket
policy, costs, and one-position semantics freeze before walk-forward Fold 1.
Each Discovery finalist is then refitted once on all eligible decision rows
`1..469`, whose labels are allowed to mature only through `470..489`. That
`discovery_final_fit` model is serialized as canonical JSON and hash-bound to
the finalist result before this Discovery run is complete. If there are no
finalists, the final-fit set is explicitly empty and no walk-forward data is
opened.

## 8. Sealed Walk-Forward and Holdout Plan

The following periods are registered planning state only. V2 is not currently
authorized to open them.

| Fold | OOS decisions | Outcome tail | Next fit may use matured rows through |
| --- | --- | --- | --- |
| 1 | 2023-08-03 to 2024-01-09 (`490..622`) | 2024-01-10 to 2024-02-01 | `622` |
| 2 | 2024-02-02 to 2024-07-10 (`643..775`) | 2024-07-11 to 2024-08-04 | `775` |
| 3 | 2024-08-05 to 2025-01-06 (`796..928`) | 2025-01-07 to 2025-01-29 | `928` |
| 4 | 2025-01-30 to 2025-07-06 (`949..1081`) | 2025-07-07 to 2025-07-29 | `1081` |
| 5 | 2025-07-30 to 2025-12-30 (`1102..1233`) | 2025-12-31 to 2026-01-22 | `1233` |

At each fold, only coefficients and the frozen train-defined scaler/calibrator
statistics may refit on all prior matured rows. Hyperparameters, thresholds,
features, and brackets do not refit. All five reports remain hidden until every
fold completes.

After simultaneous release, the preregistered gates alone may select at most
four holdout finalists. The final fit ends with decision ordinal `1233`; its
labels mature only through ordinal `1253`. The embargo `1254..1273`
(2026-01-23 through 2026-02-15) cannot train, select, or calibrate anything.

The still-sealed holdout has 120 decision days, `1274..1393`
(2026-02-16 through 2026-07-08), followed by outcome tail `1394..1413`
(2026-07-09 through 2026-07-31). It may run once after the finalist set and
final fitted artifacts are hashed and frozen.

## 9. Selection, Multiplicity, and Stop Rules

V1's 216 candidates remain predecessor exposure. All twelve V2 candidates,
model arguments, margins, feature sets, seeds, and all 49 barrier cells remain
in the multiplicity ledger. Renaming a candidate or moving it to another
campaign does not reset that history.

A candidate cannot advance on classification accuracy or AUC. It must have
support in every inner OOS interval, positive fully loaded Baseline EV,
positive Moderate total and calendar-month-loaded PnL, Moderate profit factor
of at least 1.10, worst inner-OOS Moderate EV of at least -2 ticks, nonnegative
Severe EV, and a preregistered stable connected barrier region. The
seven-of-nine neighbor rule and a nine-cell minimum component remain in force.

The one-sided 95% lower confidence bound for net EV must be positive under an
exact 10,000-replicate Politis-Romano stationary block bootstrap. Each fold
uses its OOS decision interval plus its 20-day outcome tail, producing exact
117/117/137-day calendars. A closed trade's net ticks and fill count are
assigned to its exit active date. The artifact stores those exit dates sparsely;
validation expands them onto the exact frozen fold calendars and inserts zero
net ticks and zero fills for every omitted date. The resampled statistic is
`sum(net ticks) / sum(fill count)` using aligned vectors. NumPy
`Generator(PCG64(20260809))` draws fold-local circular stationary blocks with
restart probability `1/10`; the same resample indices are reused for every
cell. The lower bound is the 500th one-indexed value of the 10,000 uncentered
bootstrap EVs. A zero-fill replicate is negative infinity for that bound. The
one-sided null is centered at observed EV and uses the plus-one formula
`(1 + exceedances) / 10001`; zero-fill replicates count as exceedances.
The GLOBAL artifact carries the exact three fold calendars and their frozen
canonical SHA. Before publication, terminal completion, or duplicate reuse,
the validator expands the sparse daily vectors and replays the same PCG64
weights to require the recorded lower bound and effective BH p-value exactly.

Multiplicity is one family of exactly 804 tests: all 216 exposed V1 variants
enter with `p=1`, followed in canonical candidate/TP-index/SL-index order by all
588 V2 Moderate cells; an ineligible cell also has `p=1`. Benjamini-Hochberg
uses `q=0.05`. Core surface positivity is joint positive Baseline EV plus
positive Moderate EV and net PnL. The neighbor median is Moderate EV, and the
orthogonally connected component must still contain at least nine cells after
BH. The representative is the Manhattan medoid of the largest eligible
component. Component and maximum-four finalist ties use the exact frozen
economic order ending in smaller SL, smaller TP, and canonical ascending
candidate key. No one inner OOS block or execution contract may contribute
more than 50% of positive gross profit.

If no candidate passes Discovery, V2 stops without opening walk-forward.
Coefficient-sign reversals are recorded as diagnostic evidence only and do not
alter the mechanical terminal decision. Economics concentrated in one period,
a convergence failure, fewer than four distinct realized distances on either
barrier axis, or an isolated profitable cell is rejection evidence, not
permission to change the model.

## 10. Frozen Identities

```text
config file SHA-256       8408a349ac2cd595e2104201185b361a5a58c7b24182babafe29e66f5c93a6e9
config semantic SHA-256   7b2d5a1e70d59b97e699d0ee479670937975ba5bcd73bc003211a1bb856e84ba
candidate catalog SHA-256 3e24dc08e9027ec604b5ab433368a54c4f7a4c89577599b79de372f62262120d
campaign definition SHA   4502e2ec1c40f344fce27066223a25e6b2f7456736e09fe0d96faab4171134f9
outer split SHA-256        5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043
nested V2 split SHA-256    9a4833aa53fe03788ddf224efcd24abbcc492498b915f174b6473f9c37f3fc8a
bootstrap calendar SHA-256 0f00faa36d08feebec1fce003268823ff02aa52b9817a84edbfcc8f863a324f1
```

Changing any listed policy or identity requires V2 to remain unexecuted or to
close under its existing identity before a new version is registered.
