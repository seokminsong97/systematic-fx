# Integration Tests

Tests here may read tiny committed/generated Parquet fixtures or a disposable
PostgreSQL database. They must never scan the full local dataset by default.
