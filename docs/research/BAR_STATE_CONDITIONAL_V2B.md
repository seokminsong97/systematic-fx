# Bar State-Conditional Model V2B

- Status: frozen engineering successor; execution awaits control-plane governance
- Campaign key: `bar_state_conditional_v2b`
- Authorized stage: Discovery only
- Predecessor: `bar_state_conditional_v2a`
- Amendment scope: FEATURE Parquet list-child field names only
- Scientific policy change: none

## 1. Decision

V2B preserves V2A's complete research question, twelve candidate definitions,
50,000-iteration model policy, data, features, labels, chronological splits,
prediction thresholds, economic replay, costs, bootstrap, multiplicity family,
selection gates, and deterministic seed. It changes only the Arrow construction
of the two nested string-list fields in the FEATURE evidence schema:
`feature_names` and `values_hex` explicitly name their list child `element`.

This is an engineering correction, not a result-driven model amendment. V2A
completed its in-memory Discovery engine twice but failed while verifying the
first staged FEATURE Parquet. No governed FEATURE, LABEL, MODEL, OOS_TRADE,
GLOBAL_RESULT, or TERMINAL_RESULT evidence was published. The error and schema
metadata were sufficient to select this fix; no economic value or finalist
result was inspected. Walk-forward and holdout remained unopened.

V2A remains immutable under its exact code snapshot and attempt history. A
source correction cannot be retried under that frozen identity, so V2B receives
new campaign, config, experiment, engine, RunSpec, and artifact namespaces.

## 2. Exact Failure and Correction

The first failed V2A payload was:

```text
kind / split / shard       FEATURE / discovery / 0
artifact suffix            tf0300_fsmorphology_discovery
row count                  98,533
logical feature group      300 seconds x MORPHOLOGY
expected schema SHA-256    d2aca906686ec49f725f215c3130cc179ca79c25033ed74443fea34b5a61413d
round-trip schema SHA-256  da7e500759276e85483f070451595eb083f3c15e76541bc2a2bd86c6483ebef3
exception                  staged Parquet differs from the artifact identity
```

The table schema used PyArrow's implicit `item` child name. Parquet reopened
the list children as `element`. PyArrow schema equality returned true, but the
lossless serialized schema bytes—and therefore their SHA-256 identities—were
different. Publication stopped before the content-addressed directory was
created. The anonymous staged file had link count zero and was closed during
exception unwinding; no orphan exists.

V2B constructs both fields as
`pa.list_(pa.field("element", pa.string()))`. Its pre-write FEATURE schema SHA
is `da7e500759276e85483f070451595eb083f3c15e76541bc2a2bd86c6483ebef3`,
and an exact Parquet round trip retains that SHA. V2 and V2A continue to use
their historical implicit `item` schema under their profiles.

## 3. Amendment Boundary

| Identity or policy | V2A | V2B |
|---|---|---|
| Campaign/config/artifact namespace | `bar_state_conditional_v2a` | `bar_state_conditional_v2b` |
| Engine version | `bar_state_conditional_discovery_v2a` | `bar_state_conditional_discovery_v2b` |
| Model `max_iter` | 50,000 | unchanged |
| Candidate keys and definitions | twelve `bsv2_*` | byte-identical canonical documents |
| Solver / tolerance / seed | `saga` / `1e-8` / `20260809` | unchanged |
| FEATURE list child names | implicit `item` | explicit `element` |
| LABEL/MODEL/OOS/GLOBAL/TERMINAL schemas | frozen | unchanged |
| Economic and selection policies | frozen | unchanged |

No fallback or permissive schema comparison is authorized. V2B retains the
strict serialized-schema hash check; it makes the declared schema canonical
before writing instead of weakening verification after writing.

## 4. Exact Predecessor Gate

V2B may start only if the control plane proves:

```text
campaign                 bar_state_conditional_v2a
campaign definition SHA  8a332ad6998bb8bf48c3de94bc0ca660905a08acb848580ee5e31d9c42f8033c
executed code commit      8688c7efb298f9644ee3821ce575349c446c6998
gate policy               REQUIRE_EXACT_FAILED_PREDECESSOR_ATTEMPTS_1_AND_2_WITH_NO_GOVERNED_EVIDENCE
```

The gate requires exactly twelve registered, prebound V2A trials and exactly
two failed attempts per RunSpec: attempt numbers 1 and 2, for 24 attempts total.
Every attempt must have a finish time, nonblank error, and null result,
trade-ledger, and reuse artifact IDs. Campaign and experiment must remain
frozen, holdout and close timestamps must be null, and all governed artifact
link roles must have count zero. The two preregistration artifacts are allowed
only under their exact V2A identities. The gate is metadata-only and opens no
result artifact.

## 5. Unchanged Scientific Identities

```text
raw source manifest SHA   14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de
bar dataset manifest SHA  e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc
outer split SHA           5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043
nested split SHA          9a4833aa53fe03788ddf224efcd24abbcc492498b915f174b6473f9c37f3fc8a
bootstrap calendar SHA    0f00faa36d08feebec1fce003268823ff02aa52b9817a84edbfcc8f863a324f1
model policy SHA          844cd3964e2871fecd13b7f7a76f07016b150b853c290c4188e275cd2226874f
candidate catalog SHA     97bbdacd0d655a1ca4e81085f3f25fb32da0bf31329bbd670ba89778611084d6
```

V2B's twelve candidate-definition hashes are exactly V2A's:

```text
bsv2_tf0300_fsmorphology_cm005 ef9d158d5909beaee7727aa5c71c99be2c44053399325c0438f508cfa0742eda
bsv2_tf0300_fsmorphology_cm010 62fa347ad4d2824e3220df29834bf4bfedd58d9df16c161b95fbfd2ab36defb7
bsv2_tf0300_fsmorphology_cm015 2620affd7d6bc99001b667d52173d90b24ee379134c6053ed27f6cad52ee4d6a
bsv2_tf0300_fsstate_cm005      315c7ac44d828afe96f4a3ec2eb38e047fe7a2e7c9c268dabe01f557807383ac
bsv2_tf0300_fsstate_cm010      6d8c80b71bccb9d25c69a173585c9dfe47a888a0fe5918240f0e95063d69035b
bsv2_tf0300_fsstate_cm015      b8530e604700b64a8e39cee7e4c6719bfd1294c8f4c64e25345a731442301ec0
bsv2_tf1800_fsmorphology_cm005 eb5404c6a507b05d243fdb1e81aa8ab9a93cb0a3bc958321b2a12a03600e44ee
bsv2_tf1800_fsmorphology_cm010 375d9a388e1346b3557703beee061c408371683b1aa27c2d7b6fa8862ea298da
bsv2_tf1800_fsmorphology_cm015 0367e3821e20fe2eb07ec278a3d3faff2bf90e15c8d1c2b1de241763ee5cf7d3
bsv2_tf1800_fsstate_cm005      a98c2d8e60da3ffc8dbf84461d0873627dfbec47847891f23c44a6785685ae1e
bsv2_tf1800_fsstate_cm010      57f4d5577456ff4ca3f30d82bb731b07c5638fa1b5f4a86b26d039d954bd19a3
bsv2_tf1800_fsstate_cm015      696f5eac1caa452082cb51c0aef9c0f856daa96e31e89267b5d05f081242ef91
```

## 6. Frozen V2B Identities

```text
config file SHA-256       87127f274ef4cc500deede2d8031919711c042530711051ba7ec598cda4e021e
config semantic SHA-256   547f30350eb829d5cf82bef6c62e7720ac9a81511759e3a791cdeba24245ad09
candidate catalog SHA-256 97bbdacd0d655a1ca4e81085f3f25fb32da0bf31329bbd670ba89778611084d6
campaign definition SHA   cee6838d9c85498818140bd02ae92483fe17c080d4909190eb0b83f790e5bb60
FEATURE schema SHA-256    da7e500759276e85483f070451595eb083f3c15e76541bc2a2bd86c6483ebef3
```

V2B remains Discovery-only. Its execution is not authorized until profile-aware
artifact registration, predecessor governance, and database migration gates are
implemented and verified. No walk-forward or holdout access is part of this
amendment.
