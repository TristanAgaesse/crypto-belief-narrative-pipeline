# Productionization (later)

This project is currently optimized for local-first iteration. Possible future hardening:

- Move from MinIO to AWS S3 / GCS (still S3-compatible APIs for S3).
- Add dataset versioning and schema evolution strategy.
- Add orchestration deployment (Dagster on Docker/Kubernetes) for scheduled runs.
- Add retries, idempotency, and backfills.
- Add observability (structured logs, metrics, lineage).
- Add CI checks for typing, formatting, and dataset contracts.

More concrete next steps:

- **Low-latency micro-batch mode**: decouple the pipeline into separate loops to avoid rewriting full day partitions on every run.
  - Binance raw ingestion: every 1 minute (1m candles)
  - Polymarket watched prices: every 5 minutes (belief repricing)
  - Polymarket discovery: every 6 hours (new markets don’t need minute-level refresh)
  - GDELT: every 1 hour (kept off the fast path; rate limits/empties are expected)
  - Silver micro-batch transforms: every 5 minutes (process only new batches)
  - Gold live signals: every 5 minutes (recent-window scoring)
  - Label maturation: every 1 hour (forward labels naturally lag)
  - Soda checks: every 30–60 minutes (too heavy for per-minute hot path)
  - Reports: daily/manual (human-facing)

- **Kubernetes/Docker deployment**: containerize collectors/transforms and run Dagster as a service.
- **Multi-pipeline layout**: keep a single Dagster “control plane” (webserver/daemon + Postgres) and add pipelines as separate user-code images exposed over gRPC; this mirrors “Airflow triggers jobs in containers” but is simpler to operate.
- **Object-store-native analytics**: keep DuckDB external views over Parquet, or migrate gold/silver to Iceberg/Delta for stronger table semantics.
- **Metrics and alerting**: Prometheus/Grafana dashboards; alerts on critical data issues (e.g., empty candles/belief snapshots).
- **Lineage**: OpenLineage/Marquez if cross-system lineage becomes necessary.
- **Source-level budgets**: rate-limit budgets and freshness SLAs per source (Polymarket/Binance/GDELT).
- **Fallback narrative providers**: treat GDELT as optional; add alternative news/attention feeds for reliability.
