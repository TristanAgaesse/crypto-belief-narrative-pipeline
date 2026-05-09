# crypto-belief-narrative-pipeline

Local-first, S3-compatible data pipeline for testing whether **Polymarket belief repricing** + **GDELT narrative acceleration** + **Binance crypto price underreaction** can produce short-horizon research signals for **BTC, ETH, and SOL**.

## Hypothesis (alpha)
If (a) prediction markets reprice beliefs, and (b) narratives accelerate attention/positioning, and (c) liquid crypto prices underreact over short horizons, then **joint belief+narrative shocks** may identify **short-horizon drift/mean-reversion regimes** worth researching.

This project is **research infrastructure**, not an execution system: it is designed to make data collection, storage, and repeatable experiments easy and auditable.

## Reviewer quick path (deterministic, no live APIs)

If you're evaluating this submission, run **only these commands**:

```bash
cp .env.example .env
make setup
make check

make minio-up
make ensure-bucket

# One command runs sample → silver → gold → DQ → issues end-to-end.
python -m crypto_belief_pipeline.cli pipeline run --date 2026-05-06 --mode sample
```

Expected artifacts:

- MinIO lake — **dedicated sample bucket** (`crypto-belief-lake-sample` by default):
  - `raw/...`, `bronze/...`, `silver/...`, `gold/...` (canonical layout, no key prefix)
- Local filesystem reports:
  - `reports/data_issues.md`, `reports/data_issues.json`
  - `reports/soda_scan_summary.json`, `reports/soda_scan_output.txt`

Sample mode is isolated by **bucket**, not by key prefix. The CLI auto-creates
the configured `sample.sample_lake_bucket` (must differ from the live
`S3_BUCKET`) so sample I/O can never land in the live lake.

Other useful entry points:

| Goal | Command |
|---|---|
| Run unit tests | `make test` |
| Lint | `make lint` |
| Typecheck | `make typecheck` |
| Lint + typecheck + tests (CI parity) | `make check` |
| Sample pipeline only (no DQ/issues) | `python -m crypto_belief_pipeline.cli pipeline run --mode sample --skip-dq --skip-issues` |
| Live (real APIs) end-to-end | `python -m crypto_belief_pipeline.cli pipeline run --date $(date +%F) --mode live` |
| Live, partial sources only (raw + bronze + silver) | `python -m crypto_belief_pipeline.cli pipeline run --date $(date +%F) --mode live --sources binance,polymarket --skip-gold --skip-dq --skip-issues` |
| Always-on Dagster scheduler | `make dagster-up` then open `http://localhost:3000` |
| Rolling Dagster image deploy (user-code; see docs for flags) | `make deploy` or `make deploy-persist` |

For deeper docs, see:
[`docs/architecture.md`](docs/architecture.md),
[`docs/reproducibility.md`](docs/reproducibility.md),
[`docs/limitations.md`](docs/limitations.md),
[`docs/productionization.md`](docs/productionization.md),
[`docs/gold_features.md`](docs/gold_features.md),
[`docs/dagster_jobs.md`](docs/dagster_jobs.md),
[`docs/deploy.md`](docs/deploy.md).

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
make check
```

Notes:
- `make run-sample` writes to the **dedicated sample bucket** configured at
  `sample.sample_lake_bucket` in `config/runtime.yaml` (default
  `crypto-belief-lake-sample`). Keys mirror the live layout
  (`raw/...`, `bronze/...`, `silver/...`, `gold/...`).

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
- Alternative.me Fear & Greed Index

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

Run the full live pipeline (ensure bucket, fetch live raw, normalize to bronze + silver, then gold + DQ + issues):

```bash
make minio-up
make ensure-bucket
RD=$(date +%F)
. .venv/bin/activate
python -m crypto_belief_pipeline.cli pipeline run --date "$RD" --mode live
```

Operator-friendly one-shot pipeline (recommended CLI interface):

```bash
. .venv/bin/activate
make minio-up
make ensure-bucket
RD=$(date +%F)
python -m crypto_belief_pipeline.cli pipeline run --date "$RD" --mode live --sources polymarket,binance --skip-gold --skip-dq --skip-issues
```

The deterministic, network-free path (`make run-sample`) continues to work and is the recommended evaluation path. Live collection is optional — its purpose is to prove the same raw contracts work on real APIs.

#### Partial-source semantics (`--sources`)

`--sources` selects which collectors run **and** which raw inputs are normalized.
Unselected sources are explicitly skipped — there is **no fallback** to default
raw keys. This means `--sources binance` will never silently mix today's fresh
Binance raw with yesterday's stale Polymarket raw. The result includes
`__sources_processed__` and `__sources_skipped__` for observability.

Gold (and the downstream DQ + issues stages) requires **Polymarket + Binance**
so belief and candle silver exist. **GDELT is optional**: you can run
`--sources polymarket,binance` and still produce gold; narrative-derived columns
are null or defaulted when narrative silver is empty. When GDELT is not
selected, the live pipeline writes **explicitly empty** narrative silver for
that `run_date` so gold never reads stale narrative partitions from an earlier
run. Partial `--sources` that omit Polymarket or Binance still fail fast for
gold/DQ/issues unless `--skip-gold --skip-dq --skip-issues` are all passed.
For narrative-inclusive end-to-end runs use `--sources all` (the default) or
omit `--sources` entirely.

#### DQ over microbatch silver

`dq run` and `issues detect` resolve silver partitions using a layout-agnostic
glob (`silver/<dataset>/date=YYYY-MM-DD/**/*.parquet`). Both layouts work
without configuration:

- CLI / sample / live live runs write `silver/<dataset>/date=.../data.parquet`.
- Dagster microbatch assets write `silver/<dataset>/date=.../hour=HH/batch_id=*.parquet`.

Gold remains a single canonical `data.parquet` per partition.

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

Materialize via Dagster CLI (sample-shaped job; requires `sample.sample_enabled=true` in `config/runtime.yaml` for `raw_sample_inputs`):

```bash
. .venv/bin/activate
RUN_DATE=2026-05-06 make dagster-materialize-sample
# equivalent:
dagster asset materialize -m crypto_belief_pipeline.orchestration.definitions \
  --job full_stack__sample__manual_job --partition 2026-05-06
```

`full_stack__sample__manual_job` includes **live** raw collector assets as upstream dependencies of bronze/silver (`raw_polymarket`, `raw_binance`, `raw_gdelt`). It is **not** a fully offline path. For deterministic offline sample data + gold + DQ, use `make full-sample` or `python -m crypto_belief_pipeline.cli pipeline run --mode sample`.

To materialize only deterministic sample raw JSONL into the configured sample bucket:

```bash
dagster asset materialize -m crypto_belief_pipeline.orchestration.definitions \
  --select raw_sample_inputs --partition 2026-05-06
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
- **GDELT is optional by design**: it is rate-limited and may return zero rows for sparse narratives, or be omitted via `--sources`. The pipeline surfaces empty narrative silver as an **informational** data issue (`narrative_counts_empty`) and does **not** block gold, DQ, or issues.
- **Lakehouse-style DQ**: DuckDB creates **external views** over Parquet via `read_parquet(...)`. Parquet in MinIO/S3 remains the source of truth; table materialization is an opt-in debug/CI fallback.

### Gold tables

- **`training_examples`**: model/research-ready joined frame keyed by `(event_time, market_id, asset, narrative)` with belief shocks, narrative acceleration, past price reactions, forward returns at 1h/4h/24h, directional labels, the underreaction score, and a candidate flag. Use this for any modelling/backtesting research.
- **`live_signals`**: filtered subset where `is_candidate_event` is true (`belief_shock_abs_1h >= 0.08`, positive underreaction score, confidence `>= 0.6`, relevance medium or high). Use this for low-latency monitoring and signal exploration.

For definitions, interpretation, and expected ranges, see [`docs/gold_features.md`](docs/gold_features.md).
For a deterministic evaluation recipe, see [`docs/reproducibility.md`](docs/reproducibility.md).

### No-lookahead principle

Step 4 enforces a strict separation:

- Feature inputs (belief, narrative, past price reaction) only use timestamps **`<= event_time`**.
- Labels (forward returns at 1h/4h/24h) only use timestamps **`> event_time`**.

Tests in `tests/test_forward_labels_no_lookahead.py` and `tests/test_narrative_features.py` enforce this invariant.

Next steps add richer quality narratives, event-study summaries (see [docs/architecture.md](docs/architecture.md#roadmap--deferred-work)), and dashboards.

## Important disclaimer
This repository **does not claim** a profitable trading strategy. It’s a framework for testing hypotheses with better data hygiene and faster iteration.
