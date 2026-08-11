# Phase 1A P4 Paired Outcome Replay

Status: preregistered implementation contract. Economic results must not be
added to this document until the governed pair has reached one atomic terminal
release.

## Purpose and authority

This run is a Discovery-only economic screen of two signal definitions that
were registered before their outcome surfaces were replayed:

1. `p4_01_opposite_depth_depletion_continuation`
2. `p4_02_depth_resistance_reversal`

The run performs no training and does not change either rule. Its maximum
authority is `SCREENING_SURVIVOR`. It cannot confer `PASS_BACKTEST`, Paper, or
Live authority, and it does not open walk-forward or sealed-holdout data.

The source signal definitions remain children of
`phase1a_conservative_screening_v1`. The paired replay is an append-only
economic extension of that same campaign lineage; a new namespace must not
reset prior economic exposure.

## Frozen signal evidence

The canonical query order is P4-01 then P4-02. The query-definition SHA-256
values are, respectively:

- `39df10c27e6fa4c5070d16cb30b4c8085fe7774a36833c141d159284f7f3dc3e`
- `825b46856dde86f7dc75393457a71d920e1eeda896f35dcd4fd47eb5fab10207`

The ordered definition-pair SHA-256 is
`21438b3d4bdc096af3b51e585869521aa60fded860b38f72802ebb46cd39f108`.
The signal-manifest SHA-256 values are:

- P4-01: `ef89f2dcc1a42176e4570a2b63c5d554c9e0d6fa1da77256dae3907a62a3bb59`
- P4-02: `c4babe44c322d391fabd305ca28b0a3274136ff611c98e2fe962b44d3d5043f4`

The ordered signal-pair SHA-256 is
`20f0fda23014218c5d104b583387368dbb52a4dd2f303b3ddcd5e14c4f39e2a2`.
Both queries consume the same 99 canonical Discovery slices and the same
portable artifact-manifest SHA-256
`23037db1dd12784e379b76effa4f3056cec18d9ae2db7fe7e54e11f2f5424d33`.

The canonical replay-config SHA-256 values are:

- pair release policy: `d83f28fae463643fc8969f8944b41c8b87254362fe709344afb7cfd240b8ea5f`
- P4-01: `a98f0c7bcaaca70bbcfe4da7f80414a96bd664c36e025176f0163a9c2a455d25`
- P4-02: `e9b49a0f45f4988403163085d3e4cc2e960c91cf630ea6d2cc24b7ce95a64220`

| Query | Signals | LONG | SHORT | Signal dates | Contracts |
| --- | ---: | ---: | ---: | ---: | ---: |
| P4-01 | 334 | 175 | 159 | 143 | 7 |
| P4-02 | 340 | 159 | 181 | 155 | 7 |

The two rules have no common decision timestamp/contract identity. They are
still separate candidates: their direction-level economics must never be
pooled.

P4-01 replays 472 source-date/contract partitions from 2022-01-03 through
2023-08-31 under input-plan SHA-256
`7014967ae8aa63842ea17d0a12ff005b2656f540974af6ead8ec763f7ff73ba6`.
P4-02 replays 455 partitions from 2022-01-18 through 2023-08-31 under
`9b764e5dae1670f365046a21b0c1c5de563462fd69b2f2c91b3d7cbd547afe9c`.

## Replay and selection contract

Each query uses an independent portfolio state with the already-frozen Phase
1A execution, cost, barrier, censor, occupancy, and terminal-exit policies.
The implementation may share verified raw cache bytes, but it must not share
positions or occupancy between candidates.

The complete frozen surface is:

```text
2 queries × 2 directions × 3 scenarios × 22 TP × 22 SL = 5,808 summaries
674 signals × 3 scenarios × 484 cells = 978,648 detail records
```

The existing adjacent-stability selector is applied independently to the four
query-direction candidates. It requires complete 484-cell recording,
Baseline/Moderate joint-positive contiguous structure, the interior 7-of-9
neighborhood rule, positive Moderate calendar-loaded PnL, and Moderate profit
factor at least 1.05. Severe remains diagnostic.

This selector does not calculate a p-value and does not implement BH,
Bonferroni, or a bootstrap confidence test. A survivor must therefore be
described only as a `SCREENING_SURVIVOR`, never as statistically significant.

For audit, the observed outcome-cell ledger is frozen as:

```text
previous P5/P1: 2 queries × 2 directions × 484 = 1,936
current P4 pair: 2 queries × 2 directions × 484 = 1,936
cumulative observed Phase1A economic cells       = 3,872
```

The wider eleven-query fixed Discovery catalog corresponds to a potential
10,648 direction/cell ledger. Unreplayed variants remain untested; no p-values
are invented for them and the number is diagnostic lineage, not a formal BH
family.

## Atomic pair release

The two candidates are one release unit.

- Both plans are verified before either economic replay starts.
- Execution order is fixed as P4-01 then P4-02.
- P4-02 execution is unconditional and cannot depend on P4-01 economics.
- Candidate result artifacts may be staged internally, but the ordinary
  candidate completion API must reject P4.
- One serializable pair-finalization transaction must append both result
  artifacts, all 5,808 cell summaries, all four directional decisions, both
  successful attempt/manifest transitions, and the released pair record.
- PostgreSQL must recompute every P4 cell and decision digest and each ordered
  2,904-row surface digest from the stored canonical fields; a caller-supplied
  row count or SHA string alone is not release evidence.
- A deferred database constraint must reject any commit containing only one
  successful P4 member, fewer than four decisions, or no released pair row.
- Any member failure before release fails the whole pair without an official
  economic release. Exact retry preserves prior failed attempts and reuses only
  content whose full identity verifies.
- An interrupted `PREPARED` pair may resume the same `QUEUED`/`RUNNING`
  members. Once the singleton pair is `RELEASED`, a different member or
  RunSpec fingerprint is rejected before replay rather than creating a second
  economic look.
- Duplicate success is accepted only after reloading both immutable result
  files, reconstructing all stored cells, re-deriving all four decisions, and
  revalidating the RunSpec, input, detail-shard, and checkpoint lineage.

The official CLI and report expose no single-member execution or result path.
PostgreSQL superuser access can still inspect internal staging; the operational
contract therefore also forbids operator or AI inspection between members.
Cryptographic blinding would require a separately privileged worker and is not
claimed here.

## Interpretation and next boundary

- Zero survivors closes the P4 pair as a Discovery rejection.
- If one to four candidates survive, every survivor is frozen together; no
  relative ranking may be used to discard another survivor.
- No walk-forward step may begin until a new immutable authorization/config is
  written. Any later sealed-holdout family must use a preregistered
  multiplicity correction across all admitted finalists.

Diagnostic thirds of the 99 slices may be reported for concentration, but
they are not OOS folds and cannot alter thresholds, TP/SL selection, or the
terminal decision.
