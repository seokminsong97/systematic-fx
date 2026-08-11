# Bar State-Conditional Model V2A

- Status: frozen optimizer-cap amendment; Discovery execution not yet authorized
- Campaign key: `bar_state_conditional_v2a`
- Authorized stage: Discovery only
- Predecessor: `bar_state_conditional_v2`
- Amendment scope: optimizer `max_iter` cap only
- Qualification: train-only; no OOS economics, walk-forward, or holdout access

## 1. Decision

V2A preserves the complete V2 research question, candidate catalog, feature and
label definitions, chronological splits, prediction thresholds, economic replay,
costs, multiplicity family, gates, ranking, tolerance, solver, objective, and
random seed. It changes only the administrative campaign namespace and the
SAGA optimizer iteration cap from 5,000 to 50,000.

This is not a result-driven hyperparameter search. V2 stopped at a deterministic
training fit because the frozen 5,000-iteration cap emitted a convergence
warning, which is a hard failure under the preregistration. The failure occurred
before signal decisions, OOS portfolio economics, GLOBAL results, or TERMINAL
results were computed or published. Walk-forward and holdout remained sealed.

The original V2 campaign stays closed under its exact configuration, candidate
definitions, code commit, and artifact namespace. V2A receives new identities
even though its canonical candidate keys remain `bsv2_*`.

## 2. Exact Amendment Boundary

| Identity or policy | V2 | V2A |
|---|---|---|
| Campaign/config/artifact namespace | `bar_state_conditional_v2` | `bar_state_conditional_v2a` |
| Experiment namespace | `bar_state_conditional_v2:experiment:frozen_candidate_catalog:v1` | `bar_state_conditional_v2a:experiment:frozen_candidate_catalog:v1` |
| Engine version | `bar_state_conditional_discovery_v2` | `bar_state_conditional_discovery_v2a` |
| `max_iter` | 5,000 | 50,000 |
| Candidate keys | twelve `bsv2_*` keys | same twelve keys |
| Solver / C / l1 ratio | `saga` / 0.1 / 0.5 | unchanged |
| Tolerance / seed | `1e-8` / `20260809` | unchanged |

For every candidate, canonical V2 and V2A candidate documents are exactly equal
after replacing only
`model_policy.sklearn_arguments.max_iter`. Candidate and model IDs are not
renamed. Campaign, experiment, config, lineage, RunSpec, and artifact identities
provide version separation.

No fallback is authorized. A warning or `n_iter >= 50000` remains a hard
candidate failure. The cap may not be raised again inside this campaign.

## 3. Train-Only Qualification Evidence

The cap was qualified on the exact fit that stopped V2:

```text
model group       300 seconds x STATE
inner fold        discovery_inner_2
training dates    2022-01-03 through 2022-09-12
label maturity    through 2022-10-05
active days       215
training rows     26,735
features          18
class counts      CENSORED=99, DOWN_FIRST=14,085, UP_FIRST=12,551
training-row SHA  d860672ce1f0496284596974d36f07f30897d9891751c2d8760e67328da6b3e0
```

Each diagnostic was a fresh fit from the same rows with the frozen scaler,
objective, solver, tolerance, and seed:

| `max_iter` | `n_iter` | Warning | Disposition |
|---:|---:|---|---|
| 5,000 | 5,000 | yes | original V2 hard failure |
| 25,000 | 25,000 | yes | insufficient diagnostic cap |
| 50,000 | 33,542 | no | selected smallest qualified cap |
| 100,000 | 33,542 | no | confirmation only |

The 50,000 and 100,000 fits produced exactly equal coefficient and intercept
arrays. Their diagnostic hashes are:

```text
coefficient SHA-256 22691a2e3a322cfaca78db45e01a44d63134f36d600ac73861d2ed6c8cf43a55
intercept SHA-256   dde5a31d4a64146b74f33d8b0cf3dde9a98945a1ae706c97c36f24adb9a96d99
```

These are SHA-256 digests of NumPy float64 C-order raw array bytes under NumPy
2.5.1, scikit-learn 1.9.0, Python 3.12.13, and
`macOS-26.5.1-arm64-arm-64bit`. They are diagnostic runtime evidence, not
portable canonical model-artifact identities. Published models continue to use
canonical JSON with hexadecimal floats.

Qualification did not inspect an OOS score, trade, PnL, p-value, finalist label,
walk-forward value, or holdout value. It answered only whether an unchanged
optimizer reached its existing tolerance before a fixed computational cap.

## 4. Predecessor Gate

V2A may start only if the control plane proves this exact predecessor:

```text
campaign                 bar_state_conditional_v2
campaign definition SHA  4502e2ec1c40f344fce27066223a25e6b2f7456736e09fe0d96faab4171134f9
executed code commit      2ca2b0b6158c1d1e9d880c2ed65ec7d7582de189
gate policy               REQUIRE_EXACT_FAILED_PREDECESSOR_WITH_NO_OOS_EVIDENCE
```

The operational gate must find all twelve exact predecessor trials registered
and prebound, attempt number 1 failed, and zero governed V2 artifact links. A
successful predecessor attempt or any FEATURE, LABEL, MODEL, OOS_TRADE,
GLOBAL_RESULT, or TERMINAL_RESULT link blocks V2A. This gate is metadata-only;
it does not open any result artifact.

## 5. Unchanged Scientific Identities

```text
raw source manifest SHA   14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de
bar dataset manifest SHA  e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc
outer split SHA           5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043
nested split SHA          9a4833aa53fe03788ddf224efcd24abbcc492498b915f174b6473f9c37f3fc8a
bootstrap calendar SHA    0f00faa36d08feebec1fce003268823ff02aa52b9817a84edbfcc8f863a324f1
```

The Discovery multiplicity family remains 804 tests: 216 predecessor pattern
variants plus the same 588 state-model barrier cells. The failed V2 optimizer
did not produce a second economic family because it stopped before OOS evidence.

## 6. Frozen V2A Identities

```text
config file SHA-256       ecc4837c67e1c42ae69bfe0c74744e8aba9ba7cd99584b2dc0c091f6579f0a52
config semantic SHA-256   2e2e3c6ee68af86fffa864ce736c24802eea7901a63d4ebda583327df06f156a
model policy SHA-256      844cd3964e2871fecd13b7f7a76f07016b150b853c290c4188e275cd2226874f
candidate catalog SHA-256 97bbdacd0d655a1ca4e81085f3f25fb32da0bf31329bbd670ba89778611084d6
campaign definition SHA   8a332ad6998bb8bf48c3de94bc0ca660905a08acb848580ee5e31d9c42f8033c
```

Candidate-definition SHA-256 values in canonical order:

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

Any further optimizer, solver, tolerance, seed, feature, threshold, split, or
economic-policy change requires another registered campaign. V2A authorizes no
walk-forward or holdout access.
