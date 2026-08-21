# E2A Month-End 24-Hour Research Registration

- Campaign: `e2a_month_end_v1`
- Candidate count: 1
- Registration status: `HISTORICAL_EVIDENCE_CONFLICT`
- Maximum authority: `SHADOW_FORWARD`
- Paper/Live authority: none
- Revised: 2026-08-20

## Outcome

The handover's 47 disclosed rows and `+1,253.5` ticks at a 1.5-tick debit are
arithmetically reproducible, but not under the handover's stated calendar and execution
policies. Conditional on injecting each stored direction, every execution field that is
actually present matches under the recovered exploratory implementation, named
`LEGACY_LAB_GRID_V1` by the governed audit. Direction is not counted as a reproduced
execution field: the raw frozen signal rule disagrees on one row. The conditional
comparison scope is 42 parquet rows with entry/exit prices and gross PnL, of which 32
also disclose all integer-second decision/exit timestamps, plus five holdout summary
rows. That legacy implementation uses the final quote row from an integer second before
the second has physically completed, carries book state without enforcing the claimed
300-second age limit, accepts locked books, and uses the legacy metas-adjacency
month-end selector.

The campaign is therefore registered fail-closed. The candidate definition is frozen
without retuning, but the historical result is not labeled validated, measured-cost,
fresh OOS, `PASS_BACKTEST`, or Paper-ready.

## Independent reproduction

Run from the repository root:

```bash
.venv/bin/python scripts/audit_e2a_handover.py > /tmp/e2a-audit.json
```

The command verifies every opened raw MBP-10 file against its frozen manifest SHA-256,
reconstructs signal inputs from selected-contract raw trades, and compares the disclosed
rows field by field. A conflict is an expected exit status `2`; integrity or schema
failure raises an error.

The verified 2026-08-20 run produced:

| Check | Result |
|---|---:|
| Handover rows | 47 |
| Handover gross | +1,324 ticks |
| Handover net at 1.5 ticks | +1,253.5 ticks |
| Handover net at 10 ticks | +854 ticks |
| Direction-conditioned legacy rows matching disclosed execution fields | 47 / 47 |
| Parquet rows with price and gross fields matched | 42 / 42 |
| Rows with full integer-second timestamps matched | 32 / 32 |
| Holdout summary-only rows matched | 5 / 5 |
| Calendar-rule signals | 51 |
| Calendar-rule legacy-grid completed trades | 50 |
| Calendar-rule legacy-grid gross | +1,184 ticks |
| Calendar-rule legacy-grid net at 1.5 ticks | +1,109.0 ticks |
| Calendar-rule legacy-grid wins | 30 / 50 |
| Disclosed direction mismatches under raw signal inputs | 1 |
| Legacy trades using quote state older than 300 seconds | 6 |

The canonical audit-body SHA-256 is
`c61e39446c4eac4024abf5bfab6c410a633618c7cfaf6c9c4bc9714ff8491770`.
Its immutable canonical artifact is 7,944 bytes, content SHA-256
`4005d506a571c83ecbeede53b1b43fb2905b2b89dd2950a18b9fe302051ffdc2`, at
`data/derived/forward_validation/e2a_month_end_v1/artifacts/e2a_handover_raw_audit/sha256=4005d506a571c83ecbeede53b1b43fb2905b2b89dd2950a18b9fe302051ffdc2.json`.
The tracked LF-terminated mirror is
[`sha256=fec7e5cc...json`](evidence/e2a_month_end_v1/sha256=fec7e5cc9fc41421c1dab929c673e4b6041a112603e880fa66535c5675e76a69.json),
7,945 bytes with byte SHA-256
`fec7e5cc9fc41421c1dab929c673e4b6041a112603e880fa66535c5675e76a69`.
The output records `raw_source_sha256_verified=true` and binds all twelve handover
source artifacts.

## Conflicts found

### Calendar/event count

The disclosed 47 are the 42 parquet rows produced by the legacy adjacency selector plus
five calendar-selected holdout rows. Applying the frozen calendar rule to every disclosed
window adds signals on:

- 2022-07-29
- 2023-04-28
- 2025-08-29
- 2025-11-28

The first three produce compatibility-grid trades; 2025-11-28 has no acceptable entry.
Consequently, the handover statement that the recovered events were already included in
the 47-event totals conflicts with the disclosed trade artifacts.

### Signal direction

For 2022-03-31, the disclosed row is short. The stated rule uses the first eligible March
date, 2022-03-01, whose month-open value is 22,510 ticks; the last physical trade at or
before 15:00 London is 22,243. The frozen direction is therefore long. The exploratory
study instead obtained its month-open context from 2022-03-08 and read the close of the
15:00:00 one-second bar, including a trade after the decision instant.

### Execution

The exact 47-row match is a compatibility result, not an endorsement of its execution
semantics. Six boundary exits carry quote state for roughly two to four hours. The
2023-09 horizon exit also accepts a locked 21,214 / 21,214 book; the first later
uncrossed quote is 21,213 / 21,214. Most compatibility entries use a physical quote
arriving later within the integer second at which it is treated as available.

The handover does not define reset/recovery semantics precisely enough to name one
strict physical replay as uniquely authoritative. A governed physical replay must freeze
that policy before its results can replace the compatibility audit.

### Strict physical-row diagnostic

A second audit removes the legacy one-second quote grid and consumes the first clean
physical selected-contract row in the frozen entry window. It rejects locked books,
multi-hour fill-forward, bad timestamps, snapshots, and maybe-bad-book rows. Because the
handover did not freeze reset recovery, this diagnostic names its policy
`LAB_MINIMAL_NEXT_CLEAN_ROW_REARM_NOT_REPO_GOVERNED` and has audit-only authority.

```bash
.venv/bin/python scripts/audit_e2a_strict_physical.py > /tmp/e2a-strict-audit.json
```

The verified result is:

| Check | Result |
|---|---:|
| Calendar-rule signals | 51 |
| Completed physical-row trades | 50 |
| Gross | +1,043 ticks |
| Net at historical 1.5-tick diagnostic debit | +968.0 ticks |
| Wins | 29 / 50 |
| Long / short | 25 / 25 |
| Exposed handover rows completed | 47 / 47 |
| Exposed gross | +993 ticks |
| Exposed net at 1.5 ticks | +922.5 ticks |
| Exposed gross mismatches | 36 / 47 |
| Entry-price mismatches | 20 / 42 |
| Exit-price mismatches | 18 / 42 |
| Entry same-integer-second matches | 42 / 42 |
| Exit same-integer-second matches | 21 / 32 |
| Exact physical nanosecond matches | 0 / 42 entry; 0 / 32 exit |
| Added 2025-11-28 opportunity | `ENTRY_NO_FILL` |

The strict audit-body SHA-256 is
`15ee2b22670d73402ce4896a782fe6f0b76d1159c6f54a4910bb1ec64b033b60`.
Its immutable canonical artifact is 45,523 bytes, content SHA-256
`be211e9c948ea4ac94c8acc8c1ca42545047f716584864b1e29cae2a1b105686`, at
`data/derived/forward_validation/e2a_month_end_v1/artifacts/e2a_strict_physical_audit/sha256=be211e9c948ea4ac94c8acc8c1ca42545047f716584864b1e29cae2a1b105686.json`.
The tracked LF-terminated mirror is
[`sha256=456f9ffe...json`](evidence/e2a_month_end_v1/sha256=456f9ffe39c13314bc2371a56143e5fc7b66918293fe3e552db72188822d010b.json),
45,524 bytes with byte SHA-256
`456f9ffe39c13314bc2371a56143e5fc7b66918293fe3e552db72188822d010b`.
The integer-second comparisons are the maximum resolution disclosed by the handover;
the exact nanosecond comparison is diagnostic and is not expected to match the legacy
grid epoch.

### Evidence provenance

The seven disclosed precommit files match the raw-byte SHA-256 values named in the
handover. They are not, however, canonical repository ledger events: they have no common
predecessor chain, and only some later verdicts backlink earlier files. The three trade
parquets also do not share one schema, and the holdout JSON omits the midpoint/path data
needed to recompute the reported pooled permutation statistic. The artifacts are
gitignored local evidence rather than committed campaign outputs. These limitations do
not change their arithmetic, but they prevent treating the handover prose alone as a
governed preregistration proof.

## Governed candidate boundary

The candidate is the exact single rule in
[`configs/campaigns/e2a_month_end_v1.toml`](../../configs/campaigns/e2a_month_end_v1.toml).
It is isolated from all frozen intraday and all-cases catalogs. There is no parameter
surface, descendant generation, historical retuning, or authority to reopen the closed
map.

All historical data through 2026-07 are in-sample for this family. The sealed
2026-02-16 through 2026-07-08 holdout was opened on 2026-08-20 and is consumed. R1 was
discovery; R2 and R3 were preregistered validation; HO was the one-shot opened holdout.
The holdout precommit is `precommits/precommit_final.json`, SHA-256
`b1183757c4d035e540d8c9d27aa378f57899ec9b126a94576e92168312dca976`; its disclosed
result is five events, four wins, and +42.5 ticks with a supportive label but no
statistical claim.
The reported pooled `p=0.023` is not independently reproducible from the disclosed
per-trade files because the required cluster/permutation inputs are absent, so it is
retained only as a handover claim and grants no authority.

## Forward validation boundary

The 2026-08 through 2027-07 timestamps are provisional calendar opportunities, not a
claim that a scheduler, live feed, or broker adapter is installed. The first is
2026-08-31 15:00 `Europe/London`, or 2026-08-31 14:00 UTC; entry eligibility begins one
second later.

The current repository has no live market-data adapter, Paper broker adapter,
reconciliation path, operational scheduler, verified fee schedule, or August source
data. The immutable v1 plan therefore permits offline shadow observations only and can
never be armed or converted into Paper evidence. In particular, broker-fill slippage is
`NOT_OBSERVABLE`, so even twelve profitable shadow observations cannot pass the complete
Section 9 gate. Resolving these blockers requires a new content-addressed plan and the
separate Paper/OCO policy amendment described in the governing documents.
