# Campaign Configurations

Each file will freeze the eligible date range, deterministic split ID,
discovery budget, permitted hypothesis families, and sealed-holdout identity.

`pipeline_smoke_v1.toml` is deliberately marked `research_eligible = false`.
It proves ingestion and quality gates only; no statistic from it may promote a
strategy.

`phase1_discovery_v1.toml` is the DRAFT full-history campaign. It records the
fixed budgets and split policy, but keeps performance blocked until every data,
cost, execution, and sealed-split gate is complete.

`e2a_month_end_v1.toml` is an isolated, single-candidate calendar-event
registration. Its historical evidence is conflict-marked and all exposed data
are in-sample. Its maximum authority is no-order `SHADOW_FORWARD`; it has no
Paper or Live authority and does not reopen any frozen candidate catalog.
