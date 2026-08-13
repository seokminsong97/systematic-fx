# Documentation

## Governing Documents

- [`DESIGN.md`](DESIGN.md): objective, scope, phase order, and invariants
- [`RESEARCH_PLAN.md`](RESEARCH_PLAN.md): hypothesis families and research budget
- [`VALIDATION.md`](VALIDATION.md): splits, rejection criteria, and promotion gates

## Implementation and Operations

- [`PROJECT_STRUCTURE.md`](PROJECT_STRUCTURE.md): code, storage, and package boundaries
- [`DATA_SCHEMA.md`](DATA_SCHEMA.md): executable raw/catalog/database contracts
- [`RESEARCH_ENVIRONMENT.md`](RESEARCH_ENVIRONMENT.md): locked runtime, private
  PostgreSQL lifecycle, readiness commands, and recovery
- [`RESEARCH_EXECUTION_STATUS.md`](RESEARCH_EXECUTION_STATUS.md): active Phase 1
  campaign boundary, gate order, data paths, verified contracts, and blockers
- `epochs/m0a_fixture_v1.toml` plus `research/m0a`: finite-budget,
  deterministic Discovery-only daemon walking skeleton (commands and holdout
  boundary are documented in `RESEARCH_ENVIRONMENT.md`)
- `configs/data/cme_6e_reference_v1.toml` plus `research/m0b`: bounded scheduled
  CME/contract reference and real MBP-10 materialization bridge; it remains
  non-entry-eligible while point-in-time status coverage is absent

## Phase Designs

- [`PHASE_1_DESIGN.md`](phases/PHASE_1_DESIGN.md): AI research and backtesting
- [`PHASE_2_DESIGN.md`](phases/PHASE_2_DESIGN.md): platform evaluation and Paper
- [`PHASE_3_DESIGN.md`](phases/PHASE_3_DESIGN.md): controlled Live trading
- [`PHASE_4_DESIGN.md`](phases/PHASE_4_DESIGN.md): risk and capital management
- [`PHASE_5_DESIGN.md`](phases/PHASE_5_DESIGN.md): continuous strategy lifecycle

## Research Designs

- [`BAR_PATTERN_DISCOVERY_V1.md`](research/BAR_PATTERN_DISCOVERY_V1.md): frozen
  5-minute, 30-minute, and 1-hour next-open pattern campaign

Document authority is defined in `DESIGN.md`. Moving a document does not change
its authority or versioning rules.
