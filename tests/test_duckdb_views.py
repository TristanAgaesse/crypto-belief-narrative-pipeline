from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

import crypto_belief_pipeline.dq.duckdb_views as dv


class _Settings:
    aws_endpoint_url = ""
    aws_access_key_id = ""
    aws_secret_access_key = ""
    aws_region = "us-east-1"
    s3_bucket = "dummy"


def test_create_duckdb_quality_db_creates_views(tmp_path, monkeypatch) -> None:
    # Make partition_path return local prefixes rooted at tmp_path
    def fake_partition_path(layer: str, dataset: str, dt: str) -> str:
        return str(tmp_path / layer / dataset / f"date={dt}")

    monkeypatch.setattr(dv, "partition_path", fake_partition_path)
    monkeypatch.setattr(dv, "get_settings", lambda: _Settings())
    monkeypatch.setattr(dv, "_s3_uri", lambda key, bucket: key)

    run_date = "2026-05-06"
    # Create parquet files at the expected paths
    paths = {
        "silver_belief_price_snapshots": Path(
            fake_partition_path("silver", "belief_price_snapshots", run_date)
        )
        / "data.parquet",
        "silver_crypto_candles_1m": Path(
            fake_partition_path("silver", "crypto_candles_1m", run_date)
        )
        / "data.parquet",
        "silver_narrative_counts": Path(fake_partition_path("silver", "narrative_counts", run_date))
        / "data.parquet",
        "gold_training_examples": Path(fake_partition_path("gold", "training_examples", run_date))
        / "data.parquet",
        "gold_alpha_events": Path(fake_partition_path("gold", "alpha_events", run_date))
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
            assert con.execute(f"select count(*) from {name}").fetchone()[0] == 1
    finally:
        con.close()


def test_create_duckdb_quality_db_materialize_tables_optional(tmp_path, monkeypatch) -> None:
    def fake_partition_path(layer: str, dataset: str, dt: str) -> str:
        return str(tmp_path / layer / dataset / f"date={dt}")

    monkeypatch.setattr(dv, "partition_path", fake_partition_path)
    monkeypatch.setattr(dv, "get_settings", lambda: _Settings())
    monkeypatch.setattr(dv, "_s3_uri", lambda key, bucket: key)

    run_date = "2026-05-06"
    p = Path(fake_partition_path("silver", "belief_price_snapshots", run_date)) / "data.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"x": [1]}).write_parquet(p)

    # Create the other expected parquet files too
    for layer, dataset in [
        ("silver", "crypto_candles_1m"),
        ("silver", "narrative_counts"),
        ("gold", "training_examples"),
        ("gold", "alpha_events"),
    ]:
        q = Path(fake_partition_path(layer, dataset, run_date)) / "data.parquet"
        q.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(q)

    db_path = tmp_path / "quality_tables.duckdb"
    out = dv.create_duckdb_quality_db(run_date, db_path=db_path, materialize_tables=True)
    assert out.exists()

    con = duckdb.connect(str(out))
    try:
        # Physical tables should exist and be queryable.
        assert con.execute("select count(*) from silver_belief_price_snapshots").fetchone()[0] == 1
    finally:
        con.close()
