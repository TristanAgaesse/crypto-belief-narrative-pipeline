from __future__ import annotations

import polars as pl

import crypto_belief_pipeline.transform.run_sample_pipeline as rsp


def test_run_sample_pipeline_writes_expected_keys(monkeypatch) -> None:
    written_jsonl: list[str] = []
    written_parquet: list[str] = []

    class _Sample:
        sample_enabled = True
        sample_lake_bucket = None
        sample_lake_prefix = "__sample__"

    class _Runtime:
        sample = _Sample()

    def fake_write_jsonl_records(records: list[dict], key: str, bucket: str | None = None) -> None:
        written_jsonl.append(key)

    def fake_write_parquet_df(df: pl.DataFrame, key: str, bucket: str | None = None) -> None:
        written_parquet.append(key)

    monkeypatch.setattr(rsp, "write_jsonl_records", fake_write_jsonl_records)
    monkeypatch.setattr(rsp, "write_parquet_df", fake_write_parquet_df)
    monkeypatch.setattr(rsp, "get_runtime_config", lambda: _Runtime())

    out = rsp.run_sample_pipeline("2026-05-06")

    expected_raw = {
        "__sample__/raw/provider=polymarket/date=2026-05-06/sample_markets.jsonl",
        "__sample__/raw/provider=polymarket/date=2026-05-06/sample_prices.jsonl",
        "__sample__/raw/provider=binance/date=2026-05-06/sample_klines.jsonl",
        "__sample__/raw/provider=gdelt/date=2026-05-06/sample_timeline.jsonl",
    }
    expected_bronze = {
        "__sample__/bronze/provider=polymarket/date=2026-05-06/markets.parquet",
        "__sample__/bronze/provider=polymarket/date=2026-05-06/prices.parquet",
        "__sample__/bronze/provider=binance/date=2026-05-06/klines.parquet",
        "__sample__/bronze/provider=gdelt/date=2026-05-06/timeline.parquet",
    }
    expected_silver = {
        "__sample__/silver/belief_price_snapshots/date=2026-05-06/data.parquet",
        "__sample__/silver/crypto_candles_1m/date=2026-05-06/data.parquet",
        "__sample__/silver/narrative_counts/date=2026-05-06/data.parquet",
    }

    assert expected_raw.issubset(set(written_jsonl))
    assert expected_bronze.issubset(set(written_parquet))
    assert expected_silver.issubset(set(written_parquet))

    # returned mapping should include at least these dataset names
    assert {
        "raw_polymarket_markets",
        "raw_polymarket_prices",
        "raw_binance_klines",
        "raw_gdelt_timeline",
        "bronze_polymarket_markets",
        "bronze_polymarket_prices",
        "bronze_binance_klines",
        "bronze_gdelt_timeline",
        "silver_belief_price_snapshots",
        "silver_crypto_candles_1m",
        "silver_narrative_counts",
    }.issubset(set(out.keys()))
