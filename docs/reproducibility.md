## Reproducibility

- **Sample:** deterministic, no network (bundled JSONL).
- **Live:** public APIs; results vary. Kalshi: `KALSHI_TRADE_API_BASE` overrides REST root if needed.

### Sample run

Prereqs: Python 3.11+, Docker (MinIO).

```bash
cp .env.example .env
make setup && make check
make minio-up && make ensure-bucket
python -m crypto_belief_pipeline.cli pipeline run --date 2026-05-06 --mode sample
```

Or `make reviewer-demo` (bootstrap + MinIO + sample E2E).

**Artifacts:** sample bucket (`sample.sample_lake_bucket` in `config/runtime.yaml`) with canonical keys (`raw/`…`gold/`); local `reports/*`. Sample bucket must differ from live `S3_BUCKET` (CLI enforces).

**Wiring:** Sample mode threads one bucket override through gold, Soda, and issues. Live `--sources` skips unselected sources (no default raw fallback). Optional GDELT: empty narrative silver for that date when skipped. DQ/issue code globs `**/*.parquet` (CLI single file or Dagster microbatches).

### Lockfile

Repro installs use **`requirements.lock.txt`** (CI + Docker). No editable VCS lines. Project: `pip install --no-deps -e .` on top.

Regenerate:

```bash
python3.11 -m venv .venv && . .venv/bin/activate
pip install --upgrade pip && pip install -e ".[dev]"
pip freeze --exclude-editable > requirements.lock.txt
```

**DuckDB:** `soda-core-duckdb` requires `duckdb<1.1.0`; newest matching release is **`duckdb==1.0.0`**. Bump when Soda relaxes the pin.

**Docker:** `Dockerfile` installs full lockfile then `pip install --no-deps -e .`. `Dockerfile_dagster` installs Dagster lines grepped from the same lockfile.

`uv pip install -r requirements.lock.txt` is supported. No `uv.lock` in repo.
