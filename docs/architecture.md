# Architecture (Step 3)

Positioning:
`MinIO/S3-compatible lake + partitioned Parquet + Polars-first processing + DuckDB for ad hoc analytical queries`.

## Components
- **MinIO**: local S3-compatible object store (Docker Compose).
- **Lake helpers**: S3 client, bucket creation, and read/write helpers.
- **Partitioning**: `{layer}/{dataset}/date=YYYY-MM-DD/...` where layer is one of `raw|bronze|silver|gold`.
- **Live collectors** (Step 3): HTTP/`gdeltdoc` clients that hit Polymarket Gamma, Binance USD-M futures, and GDELT TimelineVol, and write raw JSONL with the **same shape as Step 2 sample files**.
- **CLI**: operational + ingest commands (`check-config`, `ensure-bucket`, `print-lake-prefix`, `run-sample`, `smoke-test-apis`, `fetch-live`, `raw-to-silver`, `run-live`).

## Data flow

```
live APIs ──▶ raw JSONL ──▶ bronze Parquet ──▶ silver Parquet
sample JSONL ─┘
```

The Step 2 sample files and the Step 3 live collectors emit the same raw schemas, so the same normalizers (`normalize_markets`, `normalize_price_snapshots`, `normalize_klines`, `normalize_timeline`, `to_belief_price_snapshots`, `to_crypto_candles_1m`, `to_narrative_counts`) handle both.

## Data lifecycle
1. Raw collectors (or sample inputs) write immutable JSONL into `raw/...`.
2. Normalization builds typed, source-shaped Parquet into `bronze/...`.
3. Normalization builds research-ready tables into `silver/...`.
4. Research outputs (features/labels/event studies) land in `gold/...` (later step).

## Lake layout
All datasets are partitioned by date:
`{layer}/{dataset}/date=YYYY-MM-DD/...`

Layers:
- `raw`: immutable source-shaped JSONL, minimal assumptions
- `bronze`: typed but still source-shaped Parquet (adds `ingested_at`, keeps `raw_json`)
- `silver`: normalized research-ready Parquet (cross-source consistency)
- `gold`: future outputs (features, labels, event study tables, reports)

## Step 2 sample pipeline keys
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

## Step 3 live keys
Raw (same schemas as the sample raw JSONL above):
- `raw/provider=polymarket/date=YYYY-MM-DD/live_markets.jsonl`
- `raw/provider=polymarket/date=YYYY-MM-DD/live_prices.jsonl`
- `raw/provider=binance/date=YYYY-MM-DD/live_klines.jsonl`
- `raw/provider=gdelt/date=YYYY-MM-DD/live_timeline.jsonl`

Bronze and silver outputs use the same keys as Step 2 (one date partition per run).

## Live collectors
- **Polymarket Gamma** (`https://gamma-api.polymarket.com/markets`): discovers markets and extracts current outcome prices defensively (handles `outcomes`/`outcomePrices` as native lists or JSON strings, and the `tokens` shape). Markets are filtered using `config/markets_keywords.yaml` against `question`, `slug`, `category`, tag labels, and `description`.
- **Binance USD-M** (`https://fapi.binance.com/fapi/v1/klines`): pulls the most recent 1-minute klines for `BTCUSDT`, `ETHUSDT`, `SOLUSDT` and converts the array shape to the Step 2 raw kline schema.
- **GDELT** (`gdeltdoc.GdeltDoc`): pulls TimelineVol series for each narrative in `config/narratives.yaml`. Defaults to the previous full UTC day to avoid partial-day weirdness.

## Reuse: shared raw → silver transform
`transform/run_raw_to_silver.py` reads the four raw JSONL keys from S3 and writes bronze + silver using the same normalizers as the sample pipeline. This is the single function the live `run-live` command uses to prove the contract.