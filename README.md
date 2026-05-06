# crypto-belief-narrative-pipeline

Local-first, S3-compatible data pipeline for testing whether **Polymarket belief repricing** + **GDELT narrative acceleration** + **Binance crypto price underreaction** can produce short-horizon research signals for **BTC, ETH, and SOL**.

## Hypothesis (alpha)
If (a) prediction markets reprice beliefs, and (b) narratives accelerate attention/positioning, and (c) liquid crypto prices underreact over short horizons, then **joint belief+narrative shocks** may identify **short-horizon drift/mean-reversion regimes** worth researching.

This project is **research infrastructure**, not an execution system: it is designed to make data collection, storage, and repeatable experiments easy and auditable.

## Why this is a data-infrastructure project
- **Local-first lake**: MinIO provides an S3-compatible object store on your laptop for reproducible iteration.
- **Schema + partitioning**: consistent dataset naming and date partitions.
- **Portable IO**: the same S3 APIs work for MinIO (local) and real S3 later.
- **Testability**: core path/config logic is covered by unit tests without requiring Docker.

## Quickstart (Step 1–2)
From the project root:

```bash
cp .env.example .env
make setup
make lint
make test
```

Start MinIO and create the lake bucket:

```bash
make minio-up
make ensure-bucket
make check-config
```

Run the sample pipeline (no live APIs):

```bash
make run-sample
```

This writes a local S3-style lake layout into your MinIO bucket:
- `raw/`: immutable source-shaped JSONL
- `bronze/`: typed source-shaped Parquet
- `silver/`: normalized research-ready Parquet

MinIO console:
- `http://localhost:9001`
- Credentials: `minioadmin` / `minioadmin`

Print a clean lake prefix:

```bash
. .venv/bin/activate
python -m crypto_belief_pipeline.cli print-lake-prefix \
  --layer raw \
  --dataset provider=polymarket \
  --date 2026-05-06
```

Expected output:

```text
raw/provider=polymarket/date=2026-05-06
```

## Architecture overview (current)
- **Storage**: MinIO (S3-compatible) as the local lake.
- **Layout**: `{layer}/{dataset}/date=YYYY-MM-DD/...` where `layer ∈ {raw, bronze, silver, gold}`.
- **Interfaces**: small helpers for S3 reads/writes + a Typer CLI.

Planned sources (later steps):
- Polymarket
- Binance USD-M futures klines
- GDELT TimelineVol

## Current step status
**Step 1**: scaffold + MinIO + config + lake helpers + tests + CI.

**Step 2**: committed sample inputs + raw→bronze→silver transforms + `make run-sample` (no network calls).

Next steps add collectors and transformation layers.

## Important disclaimer
This repository **does not claim** a profitable trading strategy. It’s a framework for testing hypotheses with better data hygiene and faster iteration.
