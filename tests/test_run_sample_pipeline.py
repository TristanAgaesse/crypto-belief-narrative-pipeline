from __future__ import annotations

import polars as pl
import pytest

import crypto_belief_pipeline.io_guardrails as ig
import crypto_belief_pipeline.transform.pipeline_steps as ps
import crypto_belief_pipeline.transform.run_sample_pipeline as rsp


class _LiveSettings:
    """Stand-in for ``Settings`` that only exposes ``s3_bucket`` for the validator."""

    s3_bucket = "live-bucket"


def _patch_runtime(monkeypatch, *, enabled: bool = True, bucket: str | None = "sample-bucket"):
    """Patch the io_guardrails sources of truth for sample-bucket validation.

    The sample pipeline uses :func:`resolve_sample_bucket`, which reads
    ``get_runtime_config()`` and ``get_settings()`` from ``io_guardrails``.
    Patching at that import site keeps tests insulated from cached settings.
    """

    class _Sample:
        sample_enabled = enabled
        sample_lake_bucket = bucket

    class _Runtime:
        sample = _Sample()

    monkeypatch.setattr(ig, "get_runtime_config", lambda: _Runtime())
    monkeypatch.setattr(ig, "get_settings", lambda: _LiveSettings())


def test_run_sample_pipeline_writes_canonical_keys_into_sample_bucket(monkeypatch) -> None:
    """Sample isolation must be bucket-based: keys mirror live layout, no `__sample__` prefix."""

    written_jsonl: list[tuple[str, str | None]] = []
    written_parquet: list[tuple[str, str | None]] = []

    def fake_write_jsonl_records(records: list[dict], key: str, bucket: str | None = None) -> None:
        written_jsonl.append((key, bucket))

    def fake_write_parquet_df(df: pl.DataFrame, key: str, bucket: str | None = None) -> None:
        written_parquet.append((key, bucket))

    # Raw JSONL writes are still done by the runner; bronze/silver parquet
    # writes are delegated to the shared pipeline_steps module.
    monkeypatch.setattr(rsp, "write_jsonl_records", fake_write_jsonl_records)
    monkeypatch.setattr(ps, "write_parquet_df", fake_write_parquet_df)
    _patch_runtime(monkeypatch, bucket="sample-bucket")

    out = rsp.run_sample_pipeline("2026-05-06")

    expected_raw = {
        "raw/provider=polymarket/date=2026-05-06/sample_markets.jsonl",
        "raw/provider=polymarket/date=2026-05-06/sample_prices.jsonl",
        "raw/provider=binance/date=2026-05-06/sample_klines.jsonl",
        "raw/provider=gdelt/date=2026-05-06/sample_timeline.jsonl",
    }
    expected_bronze = {
        "bronze/provider=polymarket/date=2026-05-06/markets.parquet",
        "bronze/provider=polymarket/date=2026-05-06/prices.parquet",
        "bronze/provider=binance/date=2026-05-06/klines.parquet",
        "bronze/provider=gdelt/date=2026-05-06/timeline.parquet",
    }
    expected_silver = {
        "silver/belief_price_snapshots/date=2026-05-06/data.parquet",
        "silver/crypto_candles_1m/date=2026-05-06/data.parquet",
        "silver/narrative_counts/date=2026-05-06/data.parquet",
    }

    jsonl_keys = {k for k, _ in written_jsonl}
    parquet_keys = {k for k, _ in written_parquet}
    assert expected_raw.issubset(jsonl_keys)
    assert expected_bronze.issubset(parquet_keys)
    assert expected_silver.issubset(parquet_keys)

    # Every write must target the dedicated sample bucket.
    assert all(b == "sample-bucket" for _, b in written_jsonl)
    assert all(b == "sample-bucket" for _, b in written_parquet)

    # Returned mapping carries canonical (un-prefixed) keys plus the bucket name.
    assert out["sample_bucket"] == "sample-bucket"
    assert out["silver_belief_price_snapshots"].startswith("silver/")
    assert "__sample__" not in " ".join(out.values())


def test_run_sample_pipeline_requires_sample_bucket(monkeypatch) -> None:
    _patch_runtime(monkeypatch, bucket=None)
    monkeypatch.setattr(rsp, "write_jsonl_records", lambda *a, **kw: None)
    monkeypatch.setattr(ps, "write_parquet_df", lambda *a, **kw: None)

    with pytest.raises(ValueError, match="sample_lake_bucket"):
        rsp.run_sample_pipeline("2026-05-06")


def test_run_sample_pipeline_requires_sample_enabled(monkeypatch) -> None:
    _patch_runtime(monkeypatch, enabled=False, bucket="sample-bucket")
    monkeypatch.setattr(rsp, "write_jsonl_records", lambda *a, **kw: None)
    monkeypatch.setattr(ps, "write_parquet_df", lambda *a, **kw: None)

    with pytest.raises(ValueError, match="Sample I/O is disabled"):
        rsp.run_sample_pipeline("2026-05-06")


def test_run_sample_pipeline_rejects_sample_bucket_equal_to_live_bucket(monkeypatch) -> None:
    """The sample runner must refuse to run when sample_lake_bucket == live S3_BUCKET.

    Without this, the run would write sample artifacts directly into production data.
    """

    _patch_runtime(monkeypatch, enabled=True, bucket="live-bucket")
    monkeypatch.setattr(rsp, "write_jsonl_records", lambda *a, **kw: None)
    monkeypatch.setattr(ps, "write_parquet_df", lambda *a, **kw: None)

    with pytest.raises(ValueError, match="must differ from the live S3_BUCKET"):
        rsp.run_sample_pipeline("2026-05-06")
