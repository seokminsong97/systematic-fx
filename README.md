# systematic-fx
Systematic FX research and execution platform — tick-level data pipeline, deterministic backtesting, and risk-gated execution (EUR/USD)

Data source:
[`mbo-mbp10-converter`](https://github.com/seokminsong97/mbo-mbp10-converter)

## Local development

The source dataset belongs under `data/` and is intentionally excluded from
Git. The first metadata-only check is:

```bash
PYTHONPATH=src python3.12 -m systematic_fx data inventory
```

See [`PROJECT_STRUCTURE.md`](docs/PROJECT_STRUCTURE.md) for the code, Parquet,
PostgreSQL, artifact, and test boundaries.

## Design documents

- [`Documentation index`](docs/README.md)
- [`System design`](docs/DESIGN.md)
- [`Research plan`](docs/RESEARCH_PLAN.md)
- [`Validation`](docs/VALIDATION.md)
