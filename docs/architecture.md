# Architecture (Step 1)

Positioning:
`MinIO/S3-compatible lake + partitioned Parquet + Polars-first processing + DuckDB for ad hoc analytical queries`.

## Components
- **MinIO**: local S3-compatible object store (Docker Compose).
- **Lake helpers**: S3 client, bucket creation, and read/write helpers.
- **Partitioning**: `{layer}/{dataset}/date=YYYY-MM-DD/...` where layer is one of `raw|bronze|silver|gold`.
- **CLI**: basic operational commands (check config, ensure bucket, print partition prefix).

## Data lifecycle (planned)
1. Raw collectors write JSONL/Parquet into `raw/...`.
2. Normalization builds `bronze/...`.
3. Feature tables in `silver/...`.
4. Research outputs in `gold/...`.
