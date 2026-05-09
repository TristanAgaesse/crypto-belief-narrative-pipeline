from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

import crypto_belief_pipeline.dq.duckdb_views as dv

_KALSHI_SILVER_TABLES = (
    "silver_kalshi_markets",
    "silver_kalshi_market_snapshots",
    "silver_kalshi_events",
    "silver_kalshi_series",
    "silver_kalshi_trades",
    "silver_kalshi_orderbook_snapshots",
    "silver_kalshi_candlesticks",
    "silver_kalshi_event_repricing_features",
)
_FEAR_GREED_SILVER_TABLES = (
    "silver_fear_greed_daily",
    "silver_fear_greed_regime_features",
)


class _Settings:
    aws_endpoint_url = ""
    aws_access_key_id = ""
    aws_secret_access_key = ""
    aws_region = "us-east-1"
    s3_bucket = "dummy"


def _patch_paths(monkeypatch, tmp_path: Path) -> None:
    def fake_partition_path(layer: str, dataset: str, dt: str) -> str:
        return str(tmp_path / layer / dataset / f"date={dt}")

    monkeypatch.setattr(dv, "partition_path", fake_partition_path)
    monkeypatch.setattr(dv, "get_settings", lambda: _Settings())
    monkeypatch.setattr(dv, "_s3_uri", lambda key, bucket: key)


def test_create_duckdb_quality_db_creates_views(tmp_path, monkeypatch) -> None:
    _patch_paths(monkeypatch, tmp_path)

    run_date = "2026-05-06"
    paths = {
        "silver_belief_price_snapshots": Path(
            dv.partition_path("silver", "belief_price_snapshots", run_date)
        )
        / "data.parquet",
        "silver_crypto_candles_1m": Path(dv.partition_path("silver", "crypto_candles_1m", run_date))
        / "data.parquet",
        "silver_narrative_counts": Path(dv.partition_path("silver", "narrative_counts", run_date))
        / "data.parquet",
        **{
            name: Path(dv.partition_path("silver", name.removeprefix("silver_"), run_date))
            / "data.parquet"
            for name in _KALSHI_SILVER_TABLES
        },
        **{
            name: Path(dv.partition_path("silver", name.removeprefix("silver_"), run_date))
            / "data.parquet"
            for name in _FEAR_GREED_SILVER_TABLES
        },
        "gold_training_examples": Path(dv.partition_path("gold", "training_examples", run_date))
        / "data.parquet",
        "gold_live_signals": Path(dv.partition_path("gold", "live_signals", run_date))
        / "data.parquet",
    }
    for p in paths.values():
        p.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(p)

    db_path = tmp_path / "quality.duckdb"
    out = dv.create_duckdb_quality_db(run_date, db_path=db_path, materialize_tables=False)
    assert out.exists()

    con = duckdb.connect(str(out))
    try:
        for name in paths.keys():
            row = con.execute(f"select count(*) from {name}").fetchone()
            assert row is not None
            assert row[0] == 1
    finally:
        con.close()


def test_create_duckdb_quality_db_materialize_tables_optional(tmp_path, monkeypatch) -> None:
    _patch_paths(monkeypatch, tmp_path)

    run_date = "2026-05-06"
    p = Path(dv.partition_path("silver", "belief_price_snapshots", run_date)) / "data.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"x": [1]}).write_parquet(p)

    for layer, dataset in [
        ("silver", "crypto_candles_1m"),
        ("silver", "narrative_counts"),
        *[
            ("silver", name.removeprefix("silver_"))
            for name in _KALSHI_SILVER_TABLES
        ],
        *[
            ("silver", name.removeprefix("silver_"))
            for name in _FEAR_GREED_SILVER_TABLES
        ],
        ("gold", "training_examples"),
        ("gold", "live_signals"),
    ]:
        q = Path(dv.partition_path(layer, dataset, run_date)) / "data.parquet"
        q.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(q)

    db_path = tmp_path / "quality_tables.duckdb"
    out = dv.create_duckdb_quality_db(run_date, db_path=db_path, materialize_tables=True)
    assert out.exists()

    con = duckdb.connect(str(out))
    try:
        row = con.execute("select count(*) from silver_belief_price_snapshots").fetchone()
        assert row is not None
        assert row[0] == 1
    finally:
        con.close()


def test_create_duckdb_quality_db_reads_microbatch_silver_shards(tmp_path, monkeypatch) -> None:
    """Silver views must union multiple parquet shards under a partition prefix.

    Mimics the Dagster microbatch layout where there is no top-level
    ``data.parquet`` but multiple ``hour=HH/batch_id=*.parquet`` files exist.
    """

    _patch_paths(monkeypatch, tmp_path)

    run_date = "2026-05-06"

    # Microbatch silver: two shards under hour=12 for belief; one shard under hour=13.
    belief_part = Path(dv.partition_path("silver", "belief_price_snapshots", run_date))
    h12 = belief_part / "hour=12"
    h13 = belief_part / "hour=13"
    h12.mkdir(parents=True, exist_ok=True)
    h13.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"x": [1]}).write_parquet(h12 / "batch_id=20260506T120000Z000000.parquet")
    pl.DataFrame({"x": [2]}).write_parquet(h12 / "batch_id=20260506T120500Z000000.parquet")
    pl.DataFrame({"x": [3]}).write_parquet(h13 / "batch_id=20260506T130000Z000000.parquet")

    # Other silver datasets and gold need to exist so view creation can proceed.
    for layer, dataset in [
        ("silver", "crypto_candles_1m"),
        ("silver", "narrative_counts"),
        *[
            ("silver", name.removeprefix("silver_"))
            for name in _KALSHI_SILVER_TABLES
        ],
        *[
            ("silver", name.removeprefix("silver_"))
            for name in _FEAR_GREED_SILVER_TABLES
        ],
    ]:
        q_dir = Path(dv.partition_path(layer, dataset, run_date)) / "hour=12"
        q_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(q_dir / "batch_id=20260506T120000Z000000.parquet")

    for dataset in ("training_examples", "live_signals"):
        gold_p = Path(dv.partition_path("gold", dataset, run_date)) / "data.parquet"
        gold_p.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(gold_p)

    db_path = tmp_path / "quality_microbatch.duckdb"
    out = dv.create_duckdb_quality_db(run_date, db_path=db_path, materialize_tables=False)
    assert out.exists()

    con = duckdb.connect(str(out))
    try:
        row = con.execute("select count(*) from silver_belief_price_snapshots").fetchone()
        assert row is not None
        assert row[0] == 3
    finally:
        con.close()


def test_create_duckdb_quality_db_reads_partitioned_gold_when_partition_key_provided(
    tmp_path, monkeypatch
) -> None:
    _patch_paths(monkeypatch, tmp_path)

    run_date = "2026-05-06"
    partition_key = "2026-05-06-12:00"

    # Silver (required for view creation)
    for dataset in (
        "belief_price_snapshots",
        "crypto_candles_1m",
        "narrative_counts",
        *[name.removeprefix("silver_") for name in _KALSHI_SILVER_TABLES],
        *[name.removeprefix("silver_") for name in _FEAR_GREED_SILVER_TABLES],
    ):
        p = Path(dv.partition_path("silver", dataset, run_date)) / "data.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(p)

    # Gold exists ONLY under the partitioned hourly layout.
    safe = partition_key.replace(":", "-")
    for dataset in ("training_examples", "live_signals"):
        gold_p = (
            Path(dv.partition_path("gold", dataset, run_date))
            / f"partition={safe}"
            / "data.parquet"
        )
        gold_p.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(gold_p)

    db_path = tmp_path / "quality_partitioned_gold.duckdb"
    out = dv.create_duckdb_quality_db(
        run_date,
        db_path=db_path,
        materialize_tables=False,
        partition_key=partition_key,
    )
    assert out.exists()

    con = duckdb.connect(str(out))
    try:
        row = con.execute("select count(*) from gold_training_examples").fetchone()
        assert row is not None
        assert row[0] == 1
    finally:
        con.close()


def test_create_duckdb_quality_db_empty_silver_partition_uses_typed_zero_row_view(
    tmp_path, monkeypatch
) -> None:
    """When a date partition has no Parquet objects, DuckDB must not fail on read_parquet.

    Soda/DQ still need a view with the contractual column names (zero rows).
    """

    _patch_paths(monkeypatch, tmp_path)

    run_date = "2026-05-06"
    # Deliberately omit belief_price_snapshots parquet under the partition.
    for layer, dataset in [
        ("silver", "crypto_candles_1m"),
        ("silver", "narrative_counts"),
        *[
            ("silver", name.removeprefix("silver_"))
            for name in _KALSHI_SILVER_TABLES
        ],
        *[
            ("silver", name.removeprefix("silver_"))
            for name in _FEAR_GREED_SILVER_TABLES
        ],
    ]:
        q = Path(dv.partition_path(layer, dataset, run_date)) / "data.parquet"
        q.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(q)

    for dataset in ("training_examples", "live_signals"):
        gold_p = Path(dv.partition_path("gold", dataset, run_date)) / "data.parquet"
        gold_p.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(gold_p)

    db_path = tmp_path / "quality_missing_belief.duckdb"
    out = dv.create_duckdb_quality_db(run_date, db_path=db_path, materialize_tables=False)
    assert out.exists()

    con = duckdb.connect(str(out))
    try:
        assert con.execute("select count(*) from silver_belief_price_snapshots").fetchone()[0] == 0
        cols = {
            r[1]
            for r in con.execute("pragma table_info('silver_belief_price_snapshots')").fetchall()
        }
        assert {"timestamp", "platform", "market_id", "outcome", "price"} <= cols
        assert con.execute("select count(*) from silver_crypto_candles_1m").fetchone()[0] == 1
    finally:
        con.close()


def test_create_duckdb_quality_db_honors_bucket_override(tmp_path, monkeypatch) -> None:
    """Sample mode passes ``bucket=<sample_lake_bucket>`` so silver + gold views
    must resolve through the override bucket, not the configured live bucket.

    We patch ``_s3_uri`` to embed the bucket as a synthetic root directory and
    verify all five views point at the **override** bucket subtree.
    """

    captured_buckets: list[str] = []

    def fake_partition_path(layer: str, dataset: str, dt: str) -> str:
        return f"{layer}/{dataset}/date={dt}"

    def fake_s3_uri(key: str, bucket: str) -> str:
        captured_buckets.append(bucket)
        if key.startswith("/") or "://" in key:
            return key
        return str(tmp_path / bucket / key)

    monkeypatch.setattr(dv, "partition_path", fake_partition_path)
    monkeypatch.setattr(dv, "get_settings", lambda: _Settings())
    monkeypatch.setattr(dv, "_s3_uri", fake_s3_uri)

    run_date = "2026-05-06"
    sample_bucket = "sample-bucket"

    for dataset in (
        "belief_price_snapshots",
        "crypto_candles_1m",
        "narrative_counts",
        *[name.removeprefix("silver_") for name in _KALSHI_SILVER_TABLES],
        *[name.removeprefix("silver_") for name in _FEAR_GREED_SILVER_TABLES],
    ):
        part = tmp_path / sample_bucket / "silver" / dataset / f"date={run_date}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(part / "data.parquet")

    for dataset in ("training_examples", "live_signals"):
        gold_p = tmp_path / sample_bucket / "gold" / dataset / f"date={run_date}" / "data.parquet"
        gold_p.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(gold_p)

    db_path = tmp_path / "quality_sample.duckdb"
    out = dv.create_duckdb_quality_db(
        run_date,
        db_path=db_path,
        materialize_tables=False,
        bucket=sample_bucket,
    )
    assert out.exists()

    # Every view must have been created with the override bucket; the live
    # bucket from `_Settings.s3_bucket` must not appear.
    assert captured_buckets == [
        sample_bucket
    ] * (3 + len(_KALSHI_SILVER_TABLES) + len(_FEAR_GREED_SILVER_TABLES) + 2)
    assert "dummy" not in captured_buckets

    con = duckdb.connect(str(out))
    try:
        for name in (
            "silver_belief_price_snapshots",
            "silver_crypto_candles_1m",
            "silver_narrative_counts",
            *_KALSHI_SILVER_TABLES,
            *_FEAR_GREED_SILVER_TABLES,
            "gold_training_examples",
            "gold_live_signals",
        ):
            row = con.execute(f"select count(*) from {name}").fetchone()
            assert row is not None
            assert row[0] == 1
    finally:
        con.close()
