# Architecture (Step 2)

Positioning:
`MinIO/S3-compatible lake + partitioned Parquet + Polars-first processing + DuckDB for ad hoc analytical queries`.

## Components
- **MinIO**: local S3-compatible object store (Docker Compose).
- **Lake helpers**: S3 client, bucket creation, and read/write helpers.
- **Partitioning**: `{layer}/{dataset}/date=YYYY-MM-DD/...` where layer is one of `raw|bronze|silver|gold`.
- **CLI**: basic operational commands (check config, ensure bucket, print partition prefix).

## Data lifecycle (planned)
1. Raw collectors (or sample inputs) write immutable JSONL into `raw/...`.
2. Normalization builds typed, source-shaped Parquet into `bronze/...`.
3. Normalization builds research-ready tables into `silver/...`.
4. Research outputs (features/labels/event studies) land in `gold/...`.

## Lake layout
All datasets are partitioned by date:
`{layer}/{dataset}/date=YYYY-MM-DD/...`

Layers:
- `raw`: immutable source-shaped JSONL, minimal assumptions
- `bronze`: typed but still source-shaped Parquet (adds `ingested_at`, keeps `raw_json`)
- `silver`: normalized research-ready Parquet (cross-source consistency)
- `gold`: future outputs (features, labels, event study tables, reports)

## Step 2 sample pipeline datasets
Step 2 writes the following keys for a run date (e.g. `2026-05-06`):

Raw:
- `raw/provider=polymarket/date=YYYY-MM-DD/sample_markets.jsonl`
- `raw/provider=polymarket/date=YYYY-MM-DD/sample_prices.jsonl`
- `raw/provider=binance/date=YYYY-MM-DD/sample_klines.jsonl`
- `raw/provider=gdelt/date=YYYY-MM-DD/sample_timeline.jsonl`

Bronze:
- `bronze/provider=polymarket/date=YYYY-MM-DD/markets.parquet`
- `bronze/provider=polymarket/date=YYYY-MM-DD/prices.parquet`
- `bronze/provider=binance/date=YYYY-MM-DD/klines.parquet`
- `bronze/provider=gdelt/date=YYYY-MM-DD/timeline.parquet`

Silver:
- `silver/belief_price_snapshots/date=YYYY-MM-DD/data.parquet`
- `silver/crypto_candles_1m/date=YYYY-MM-DD/data.parquet`
- `silver/narrative_counts/date=YYYY-MM-DD/data.parquet`
