# Integration Tests

Tests here may read one bounded real Parquet row group and the explicitly
configured repository-private `systematic_fx_test` PostgreSQL database. They
must never scan the full local event dataset or mutate the `systematic_fx`
research database by default.

The test bootstrap loads the ignored repository `.env` without overriding
process variables. Set both integration boundaries explicitly:

```text
SYSTEMATIC_FX_TEST_DATABASE_URL=postgresql://.../systematic_fx_test
SYSTEMATIC_FX_SMOKE_PARQUET=./data/mbp-10/2022/01/03/glbx-mdp3-20220103.mbp-10.parquet
```

The collection guard rejects any test URL whose explicit database name is not
`systematic_fx_test`. The canonical local run starts the private cluster and
creates/migrates that isolated database before pytest:

```bash
make research-ready
```
