# Feature Configurations

Each file will freeze raw input columns, event-time rules, one-second features,
five-minute aggregates, validity masks, and a feature-set version.

`mbp10_v1.toml` is the initial point-in-time contract. Changing its availability
rules or feature membership requires a new feature-set ID and rebuilt derived
partitions.

`mbp10_pilot_v1.toml` is a deliberately smaller, non-research pipeline contract.
It requires an explicit outright symbol and instrument ID, creates only observed
one-second rows and closed five-minute summaries, and must not be treated as an
implementation of the broader `mbp10_v1` design.
