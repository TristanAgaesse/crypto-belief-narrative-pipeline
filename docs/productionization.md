# Productionization (later)

Local-first today. Possible next steps:

- Cloud object store (S3/GCS); dataset versioning / schema policy.
- Dagster on K8s; retries, idempotency, structured observability.
- **Cadence sketch:** Binance 1m; Polymarket 5m + 6h discovery; GDELT 1h; silver ~5m; gold signals ~5m; label maturation 1h; Soda 30–60m; reports daily.
- Multi-pipeline: one Dagster control plane, user code per pipeline over gRPC.
- Iceberg/Delta if you need stronger table semantics than Parquet + DuckDB views.
- Alerts on critical issues; per-source rate budgets; optional narrative providers beyond GDELT.
