# Productionization (later)

This project is currently optimized for local-first iteration. Possible future hardening:

- Move from MinIO to AWS S3 / GCS (still S3-compatible APIs for S3).
- Add dataset versioning and schema evolution strategy.
- Add orchestration (e.g., cron, Airflow, Dagster) for scheduled runs.
- Add retries, idempotency, and backfills.
- Add observability (structured logs, metrics, lineage).
- Add CI checks for typing, formatting, and dataset contracts.
