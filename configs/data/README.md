# Data Contracts

This directory contains versioned requirements for raw, reference, and
qualification inputs. Actual market-data files and every row-derived output
remain below the ignored `data/` tree.

`phase1_reference_inputs_v1.toml` records the small reference-data gap that
MBP-10 alone cannot fill: point-in-time instrument definitions and trading
status. These inputs must be cataloged and hashed before they can qualify a
contract, session, roll, or eligible day.

`mbp10_structural_qc_v1.toml` freezes the complete row-group scanner contract.
Hard structural counters have zero tolerance; clock, sequence, snapshot, and
book-state observations that require later session/status interpretation are
retained as non-gating diagnostics. Changing any check list or threshold
requires a new checker/config version.

The implementation is available through `make qc`. The v1 scan completed all
1,434 source files and failed six of them on 11 clean `T/N` book mutations.
Its restart checkpoint, final JSONL, and row-level anomaly detail remain under
`data/derived/manifests/`; `make qc-register` verifies and records the immutable
result without promoting source or dataset status.

`phase1_structural_exclusions_v1.toml` freezes the conservative downstream
disposition: exclude all six source dates from the campaign-common eligible
calendar. It does not convert the full-dataset `FAIL` into a pass, and it does
not yet make the campaign research-eligible. Reconsideration requires
point-in-time `status` evidence, preferably matching MBO reconstruction, and a
new checker/config version; a hard-coded 22:00 UTC exception is prohibited.
