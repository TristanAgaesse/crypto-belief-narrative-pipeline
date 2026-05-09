from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.lake.paths import partition_path


def _as_date_str(run_date: date | str) -> str:
    return run_date.isoformat() if isinstance(run_date, date) else str(run_date)


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _configure_s3(con: duckdb.DuckDBPyConnection) -> None:
    """Configure DuckDB httpfs S3 settings for MinIO/S3-compatible access."""

    s = get_settings()
    endpoint = s.aws_endpoint_url.strip()
    if not endpoint:
        return

    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    use_ssl = endpoint.startswith("https://")
    endpoint_no_scheme = endpoint.replace("https://", "").replace("http://", "")

    # DuckDB doesn't allow prepared parameters for SET statements.
    con.execute(f"SET s3_region={_sql_quote(s.aws_region)};")
    con.execute(f"SET s3_access_key_id={_sql_quote(s.aws_access_key_id)};")
    con.execute(f"SET s3_secret_access_key={_sql_quote(s.aws_secret_access_key)};")
    con.execute(f"SET s3_endpoint={_sql_quote(endpoint_no_scheme)};")
    con.execute("SET s3_url_style='path'")
    con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")

    # Persist credentials for other DuckDB clients (e.g. Soda opening the DB separately).
    # Secrets are stored in the DuckDB database file.
    con.execute("DROP SECRET IF EXISTS crypto_lake_s3;")
    con.execute(
        "CREATE SECRET crypto_lake_s3 ("
        "TYPE S3, "
        f"KEY_ID {_sql_quote(s.aws_access_key_id)}, "
        f"SECRET {_sql_quote(s.aws_secret_access_key)}, "
        f"REGION {_sql_quote(s.aws_region)}, "
        f"ENDPOINT {_sql_quote(endpoint_no_scheme)}, "
        "URL_STYLE 'path', "
        f"USE_SSL {'true' if use_ssl else 'false'}"
        ");"
    )


def _s3_uri(key: str, bucket: str) -> str:
    if key.startswith("s3://") or key.startswith("file:") or "://" in key:
        return key
    return f"s3://{bucket}/{key}"


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _lake_key(relative_key: str) -> str:
    """Resolve a lake-relative path to its on-bucket key.

    Applies the configured ``s3_prefix`` via :func:`full_s3_key`. Sample runs
    keep the same key layout but live in a separate bucket, so no per-key
    prefix override is needed here — the bucket is overridden at the
    DuckDB-view URI level instead.
    """

    # Tests may patch `partition_path` to return absolute local filesystem paths. In that case,
    # we must not rewrite the key.
    if relative_key.startswith("/") or "://" in relative_key:
        return relative_key

    # Avoid importing through `lake.keys.full_s3_key` here so unit tests can patch
    # `get_settings()` locally without needing to satisfy pydantic env requirements.
    s = get_settings()
    prefix = (getattr(s, "s3_prefix", "") or "").strip("/")
    if not prefix:
        return relative_key
    return f"{prefix}/{relative_key.lstrip('/')}"


def _silver_partition_glob(rd: str, dataset: str) -> str:
    """Glob covering both single-file and microbatch silver layouts.

    Examples:
      - ``silver/<dataset>/date=YYYY-MM-DD/data.parquet``
      - ``silver/<dataset>/date=YYYY-MM-DD/hour=HH/batch_id=*.parquet``
    """

    base = partition_path("silver", dataset, rd)
    return f"{base}/**/*.parquet"


def _safe_partition_slug(partition_key: str) -> str:
    return partition_key.replace(":", "-")


def _gold_key(rd: str, dataset: str, partition_key: str | None) -> str:
    """Resolve a gold parquet path for either daily or hourly-partitioned layouts.

    - Daily (CLI/sample): ``gold/<dataset>/date=YYYY-MM-DD/data.parquet``
    - Hourly (Dagster): ``gold/<dataset>/date=YYYY-MM-DD/partition=YYYY-MM-DD-HH-00/data.parquet``
    """
    base = partition_path("gold", dataset, rd)
    if partition_key:
        return f"{base}/partition={_safe_partition_slug(partition_key)}/data.parquet"
    return f"{base}/data.parquet"


def create_duckdb_quality_db(
    run_date: date | str,
    db_path: str | Path = "data/quality/crypto_lake.duckdb",
    *,
    materialize_tables: bool = False,
    bucket: str | None = None,
    partition_key: str | None = None,
) -> Path:
    """Create DuckDB views (default) over lake Parquet for a given run_date.

    Default behavior is lakehouse-style: Parquet in the lake remains source of truth, and
    DuckDB exposes external views via read_parquet(...).

    Silver tables resolve to a recursive glob so they work for both single-file
    (``data.parquet``) and Dagster microbatch (``hour=HH/batch_id=*.parquet``) layouts.

    ``bucket`` overrides the lake bucket for both silver and gold lookups. Sample
    runs pass the dedicated sample bucket here; live runs leave it ``None`` and
    fall back to the configured ``s3_bucket``.

    If materialize_tables=True, the Parquet is materialized into DuckDB tables as an
    opt-in fallback for CI/debug.
    """

    s = get_settings()
    rd = _as_date_str(run_date)
    dbp = Path(db_path)
    _ensure_parent(dbp)
    target_bucket = bucket or s.s3_bucket

    silver_globs: dict[str, str] = {
        "silver_belief_price_snapshots": _lake_key(
            _silver_partition_glob(rd, "belief_price_snapshots")
        ),
        "silver_crypto_candles_1m": _lake_key(_silver_partition_glob(rd, "crypto_candles_1m")),
        "silver_narrative_counts": _lake_key(_silver_partition_glob(rd, "narrative_counts")),
        "silver_kalshi_markets": _lake_key(_silver_partition_glob(rd, "kalshi_markets")),
        "silver_kalshi_market_snapshots": _lake_key(
            _silver_partition_glob(rd, "kalshi_market_snapshots")
        ),
        "silver_kalshi_events": _lake_key(_silver_partition_glob(rd, "kalshi_events")),
        "silver_kalshi_series": _lake_key(_silver_partition_glob(rd, "kalshi_series")),
        "silver_kalshi_trades": _lake_key(_silver_partition_glob(rd, "kalshi_trades")),
        "silver_kalshi_orderbook_snapshots": _lake_key(
            _silver_partition_glob(rd, "kalshi_orderbook_snapshots")
        ),
        "silver_kalshi_candlesticks": _lake_key(_silver_partition_glob(rd, "kalshi_candlesticks")),
        "silver_kalshi_event_repricing_features": _lake_key(
            _silver_partition_glob(rd, "kalshi_event_repricing_features")
        ),
        "silver_fear_greed_daily": _lake_key(_silver_partition_glob(rd, "fear_greed_daily")),
        "silver_fear_greed_regime_features": _lake_key(
            _silver_partition_glob(rd, "fear_greed_regime_features")
        ),
    }
    gold_keys: dict[str, str] = {
        "gold_training_examples": _lake_key(_gold_key(rd, "training_examples", partition_key)),
        "gold_live_signals": _lake_key(_gold_key(rd, "live_signals", partition_key)),
    }

    con = duckdb.connect(str(dbp))
    try:
        _configure_s3(con)

        for name, glob_key in silver_globs.items():
            uri = _s3_uri(glob_key, bucket=target_bucket)
            select_expr = f"SELECT * FROM read_parquet({_sql_quote(uri)}, union_by_name=true)"
            verb = "TABLE" if materialize_tables else "VIEW"
            con.execute(f"CREATE OR REPLACE {verb} {name} AS {select_expr};")

        for name, key in gold_keys.items():
            uri = _s3_uri(key, bucket=target_bucket)
            # Use union_by_name so both single-file and partitioned gold layouts are tolerant
            # to schema evolution and match silver view behavior.
            select_expr = f"SELECT * FROM read_parquet({_sql_quote(uri)}, union_by_name=true)"
            verb = "TABLE" if materialize_tables else "VIEW"
            con.execute(f"CREATE OR REPLACE {verb} {name} AS {select_expr};")
    finally:
        con.close()

    return dbp


def open_quality_connection(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection suitable for Soda scans (httpfs + S3/MinIO configured).

    This pulls credentials/endpoint from `get_settings()` (which reads `.env`) and avoids
    hardcoding secrets in code or YAML files.
    """

    con = duckdb.connect(str(db_path))
    _configure_s3(con)
    return con
