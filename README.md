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
### Prereqs
- **Python**: 3.11+
- **Docker Desktop**: for MinIO + Dockerized Dagster

### Local (venv) install
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

### Dockerized Dagster install (always-on scheduler)

This is the “turnkey local platform” path (MinIO + Postgres + Dagster webserver/daemon + user-code gRPC):

```bash
cp .env.example .env
make dagster-up
make dagster-ensure-bucket
```

- Dagster UI: `http://localhost:3000`
- MinIO console: `http://localhost:9001`

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

**Step 3**: live collectors for Polymarket Gamma, Binance USD-M klines, and GDELT TimelineVol. Live collectors write the **same raw JSONL contracts** as the Step 2 sample files, so the existing normalizers handle both unchanged.

**Step 4**: feature engineering and labeling. Builds research-ready gold tables `training_examples` and `live_signals` from silver inputs with strict no-lookahead semantics: features only use timestamps `<= event_time`, labels only use timestamps `> event_time`.

**Step 3.5**: orchestration + monitoring + data quality. Dagster materializes partitioned assets with run metadata and schedules; Soda Core runs data-quality checks against DuckDB external views over the Parquet lake; a custom issue detector surfaces domain-specific pipeline problems (including GDELT empty days) as structured issues instead of crashes.

### Step 3 commands

Smoke-test the public APIs (does not write to the lake):

```bash
make smoke-test-apis
```

Fetch live raw data for a date and write JSONL to the lake:

```bash
RUN_DATE=$(date +%F) make fetch-live
```

Run the full live pipeline (ensure bucket, fetch live raw, transform to bronze + silver):

```bash
make minio-up
make ensure-bucket
RUN_DATE=$(date +%F) make run-live
```

Operator-friendly one-shot pipeline (recommended CLI interface):

```bash
. .venv/bin/activate
make minio-up
make ensure-bucket
RD=$(date +%F)
python -m crypto_belief_pipeline.cli pipeline run --date "$RD" --mode live --sources polymarket,binance
```

The deterministic, network-free path (`make run-sample`) continues to work and is the recommended evaluation path. Live collection is optional — its purpose is to prove the same raw contracts work on real APIs.

### Live raw keys

```
raw/provider=polymarket/date=YYYY-MM-DD/hour=HH/batch_id=..._markets.jsonl
raw/provider=polymarket/date=YYYY-MM-DD/hour=HH/batch_id=..._prices.jsonl
raw/provider=binance/date=YYYY-MM-DD/hour=HH/batch_id=....jsonl
raw/provider=gdelt/date=YYYY-MM-DD/hour=HH/batch_id=....jsonl
```

### Step 4 commands

Build gold tables from silver:

```bash
RUN_DATE=2026-05-06 make build-gold
```

This writes:

```
gold/training_examples/date=YYYY-MM-DD/data.parquet
gold/live_signals/date=YYYY-MM-DD/data.parquet
```

End-to-end sample run:

```bash
make minio-up
make ensure-bucket
make run-sample
RUN_DATE=2026-05-06 make build-gold
```

## Orchestration and Monitoring (Step 3.5)

Dagster UI (asset graph, run history, partitions, logs):

```bash
make dagster-dev
```

Then open `http://localhost:3000`.

### Dockerized Dagster (always-on scheduler)

This repo supports a local “mini platform” that looks like a simplified Airflow-on-Kubernetes pattern:
- **Control plane (always on)**: Dagster webserver + Dagster daemon + Postgres metadata DB
- **User code**: your pipeline packaged as a Docker image and exposed over gRPC to the control plane
- **Execution**: each run executes in its own short-lived container (DockerRunLauncher), which is a good stand-in for “run a container in Kubernetes” without bringing K8s into local dev

Bring everything up:

```bash
docker compose up -d --build
```

- Dagster UI: `http://localhost:3000`
- MinIO console: `http://localhost:9001`

#### Images (what they contain)

- **`crypto-belief-pipeline:local`**:
  - Python package `crypto_belief_pipeline` (collectors, transforms, assets, DQ, feature builds)
  - Dagster entrypoints: `dagster-webserver`, `dagster-daemon`, `dagster api grpc`
  - Used for:
    - `dagster-user-code-crypto-belief` (gRPC code location)
    - `dagster-webserver` + `dagster-daemon` (control plane)
    - ephemeral “run containers” launched per run by DockerRunLauncher

- **`postgres:16`**:
  - Dagster run/event/schedule storage (durable state so the daemon can schedule continuously)

- **`minio/minio:latest`**:
  - Local S3-compatible object storage used by the pipeline lake

#### What makes this scalable + flexible (multi-pipeline)

This setup scales by **adding more user-code images**, not by multiplying the control plane:
- Add a new pipeline by creating another `dagster-user-code-<name>` service built from its own image.
- Add another `grpc_server` entry in `dagster/workspace.yaml`.
- The same Dagster webserver/daemon instance can discover and orchestrate multiple code locations.

This matches “central scheduler triggers containerized jobs” while remaining local and simple.

#### Environment variables (not committed)

- `.env` is **gitignored** and can contain local credentials/overrides.
- `.env.example` is the committed template.
- In Docker, services read env vars from your local environment/`.env` at runtime; nothing secret is baked into images.

Materialize via Dagster CLI (example: sample partition):

```bash
. .venv/bin/activate
dagster asset materialize -m crypto_belief_pipeline.orchestration.definitions \
  --select raw_sample_inputs,bronze_polymarket,bronze_binance,bronze_gdelt,\
silver_belief_price_snapshots,silver_crypto_candles_1m,silver_narrative_counts,\
gold_training_examples,gold_live_signals,soda_data_quality,data_issues,markdown_reports \
  --partition 2026-05-06
```

Data quality (Soda Core over DuckDB external Parquet views):

```bash
RUN_DATE=2026-05-06 make dq
```

Or directly:

```bash
. .venv/bin/activate
python -m crypto_belief_pipeline.cli dq run --date 2026-05-06
```

Domain-specific data issue detector (writes `reports/data_issues.md` and `reports/data_issues.json`):

```bash
RUN_DATE=2026-05-06 make detect-data-issues
```

Or directly:

```bash
. .venv/bin/activate
python -m crypto_belief_pipeline.cli issues detect --date 2026-05-06
```

Notes:
- **GDELT is optional by design**: it is rate-limited and may return zero rows for sparse narratives. The pipeline should surface this as a **data issue** (high severity), not crash.
- **Lakehouse-style DQ**: DuckDB creates **external views** over Parquet via `read_parquet(...)`. Parquet in MinIO/S3 remains the source of truth; table materialization is an opt-in debug/CI fallback.

### Gold tables

- **`training_examples`**: model/research-ready joined frame keyed by `(event_time, market_id, asset, narrative)` with belief shocks, narrative acceleration, past price reactions, forward returns at 1h/4h/24h, directional labels, the underreaction score, and a candidate flag. Use this for any modelling/backtesting research.
- **`live_signals`**: filtered subset where `is_candidate_event` is true (`belief_shock_abs_1h >= 0.08`, positive underreaction score, confidence `>= 0.6`, relevance medium or high). Use this for low-latency monitoring and signal exploration.

For definitions, interpretation, and expected ranges, see [`docs/gold_features.md`](docs/gold_features.md).

### No-lookahead principle

Step 4 enforces a strict separation:

- Feature inputs (belief, narrative, past price reaction) only use timestamps **`<= event_time`**.
- Labels (forward returns at 1h/4h/24h) only use timestamps **`> event_time`**.

Tests in `tests/test_forward_labels_no_lookahead.py` and `tests/test_narrative_features.py` enforce this invariant.

Next steps add quality reports, event-study summaries, and dashboards.

## Important disclaimer
This repository **does not claim** a profitable trading strategy. It’s a framework for testing hypotheses with better data hygiene and faster iteration.
