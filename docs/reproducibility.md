## Reproducibility

This repo is designed to be reproducible in two modes:

- **Sample mode (recommended for evaluation)**: deterministic, no network calls.
- **Live mode**: hits public APIs and will vary by time/availability.
  Kalshi ingestion uses the public Trade API by default; optional env
  `KALSHI_TRADE_API_BASE` overrides the REST root if you point at demo or
  alternate endpoints.

### Sample mode (deterministic) checklist

Prereqs:
- Python 3.11+
- Docker Desktop (for local MinIO)

One-shot run (preferred — runs sample → silver → gold → DQ → issues end-to-end):

```bash
cp .env.example .env
make setup
make check

make minio-up
make ensure-bucket

python -m crypto_belief_pipeline.cli pipeline run --date 2026-05-06 --mode sample
```

Or the granular Make targets (equivalent):

```bash
make run-sample
RUN_DATE=2026-05-06 make build-gold
RUN_DATE=2026-05-06 make dq
RUN_DATE=2026-05-06 make detect-data-issues
RUN_DATE=2026-05-06 make generate-reports
```

Expected artifacts:
- Lake (MinIO) — **sample bucket** (`crypto-belief-lake-sample` by default,
  configurable at `sample.sample_lake_bucket` in `config/runtime.yaml`):
  - `raw/...`, `bronze/...`, `silver/...`, `gold/...` (canonical layout, no
    key prefix; identical to live)
- Reports (local filesystem): `reports/index.md`, `reports/soda_scan_summary.json`,
  `reports/data_issues.json`

The CLI auto-creates the sample bucket on `pipeline run --mode sample`. The
sample bucket must be **distinct** from the live `S3_BUCKET`; the CLI rejects
identical names so sample I/O cannot accidentally land in the live lake.

### How the sample / live wiring works

The CLI wraps three things into one command for `pipeline run`:

1. **Sample mode** isolates I/O at the **bucket** level. Raw, bronze, silver,
   and gold all use the canonical key layout (`raw/...`, `silver/...`,
   `gold/...`) but live in `sample.sample_lake_bucket`. The CLI threads that
   bucket name through `build_gold_tables`, `run_soda_checks`, and
   `detect_data_issues`, so every stage reads/writes the same sample bucket.
2. **Live mode** with `--sources` only normalizes selected sources; unselected
   sources are explicitly skipped (no fallback to default raw keys). This makes
   partial-source runs safe to repeat: re-running with a different `--sources`
   subset never reuses stale raw input.
3. **DQ + data-issues** reads silver via a partition glob (`**/*.parquet`),
   so single-file (`data.parquet`) and Dagster microbatch
   (`hour=HH/batch_id=*.parquet`) layouts both work without configuration.

### Dependency pinning

The project declares unpinned dependency ranges in `pyproject.toml`. For
**reproducible installs** the canonical artifact is `requirements.lock.txt`:
exact versions for both runtime and dev (lint/type/test) tooling. CI installs
from this lockfile (not from the open ranges) so a green CI commit always
reflects an exact, reproducible environment.

The lockfile must NOT contain editable VCS entries (e.g.
`-e git+https://...`) — those defeat byte-for-byte reproducibility. The
project itself is layered onto the locked environment in CI via
`pip install --no-deps -e .` so its own open ranges don't override the locked
versions.

To regenerate the lockfile after adding/upgrading a dependency:

```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -e ".[dev]"
pip freeze --exclude-editable > requirements.lock.txt
```

Commit the resulting `requirements.lock.txt` alongside the `pyproject.toml`
change so CI and any downstream consumer see the same versions.

[`uv`](https://github.com/astral-sh/uv) installs (`uv pip install -r
requirements.lock.txt`) work against the same lockfile. A standalone
`uv.lock` is intentionally not tracked in this repo so there is exactly one
source of truth for pinned versions.
