# crypto-belief-narrative-pipeline

Research pipeline: **Polymarket**, **GDELT**, **Binance USD-M** (plus optional **Fear & Greed**, **Kalshi**) → S3-style lake → **gold** tables with **point-in-time** joins. Not trading infrastructure; no performance claims.

| Area | Contents |
|------|----------|
| Path | Sample JSONL (`data/sample/`) and live collectors share raw contracts → same normalizers. `--sources` skips sources explicitly (no stale mixing). |
| Gold | `training_examples`, `live_signals`: belief shock, narrative accel, returns, labels, underreaction, candidate filter. Features ≤ `event_time`; labels > `event_time` (`tests/test_forward_labels_no_lookahead.py`). |
| Ops | Dagster (partitions, schedules), DuckDB views, Soda (`dq/checks/`), issue detector → `reports/data_issues.*`, `pipeline run` → `reports/run_summary.*`. Fear/Greed + Kalshi: [`docs/dagster_jobs.md`](docs/dagster_jobs.md). |

**Sources:** Polymarket + Binance silver required for gold (CLI); GDELT optional (null narrative features if empty). Sample I/O uses **`sample.sample_lake_bucket`** (not live `S3_BUCKET`).

**Stack:** Parquet lake, Polars, Dagster, DuckDB + Soda, Typer CLI, Pydantic config. Pins: [`requirements.lock.txt`](requirements.lock.txt) + [`docs/reproducibility.md`](docs/reproducibility.md).

## Reviewer path

```bash
cp .env.example .env
make reviewer-demo
```

Or: `make setup && make check`, `make minio-up && make ensure-bucket`, then  
`python -m crypto_belief_pipeline.cli pipeline run --date 2026-05-06 --mode sample`.

**Outputs:** sample-bucket `raw/`…`gold/`, `reports/run_summary.*`, `reports/soda_scan_*`, `reports/data_issues.*`. **CI:** `make check`.

## Commands

| | |
|-|--|
| Lint / types / tests | `make check` |
| Sample raw→silver | `make run-sample` |
| Live E2E | `python -m crypto_belief_pipeline.cli pipeline run --date $(date +%F) --mode live` |
| Dagster UI | `make dagster-dev` → http://localhost:3000 |
| Docker stack | `make dagster-up` |

**Docs:** [`docs/architecture.md`](docs/architecture.md), [`docs/limitations.md`](docs/limitations.md), [`docs/gold_features.md`](docs/gold_features.md), [`docs/deploy.md`](docs/deploy.md).
