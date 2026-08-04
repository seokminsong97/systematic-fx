# Campaign Configurations

Each file will freeze the eligible date range, deterministic split ID,
discovery budget, permitted hypothesis families, and sealed-holdout identity.

`pipeline_smoke_v1.toml` is deliberately marked `research_eligible = false`.
It proves ingestion and quality gates only; no statistic from it may promote a
strategy.

`phase1_discovery_v1.toml` is the DRAFT full-history campaign. It records the
fixed budgets and split policy, but keeps performance blocked until every data,
cost, execution, and sealed-split gate is complete.
