"""Tests for the shared per-source raw->bronze->silver step functions.

These tests intentionally mock both the normalize transforms and the parquet
writer so that they exercise only the orchestration shape (key layout, bucket
threading, returned dict shape). The actual normalize logic has its own
dedicated tests under ``tests/test_collectors_*`` and similar.
"""

from __future__ import annotations

import polars as pl

import crypto_belief_pipeline.transform.pipeline_steps as ps


def _patch_normalize_and_writer(
    monkeypatch,
) -> tuple[list[tuple[str, str | None]], dict[str, pl.DataFrame]]:
    writes_log: list[tuple[str, str | None]] = []
    writes_by_key: dict[str, pl.DataFrame] = {}
    bronze_stub = pl.DataFrame({"x": [1]})
    # Silver stubs must satisfy the dataset contracts in contracts.py because
    # pipeline_steps now validates each silver before writing.
    silver_belief = pl.DataFrame(
        {"timestamp": [], "platform": [], "market_id": [], "outcome": [], "price": []}
    )
    silver_candles = pl.DataFrame({"timestamp": [], "exchange": [], "asset": [], "close": []})
    silver_counts = pl.DataFrame({"timestamp": [], "narrative": [], "mention_volume": []})

    def fake_write(df, key: str, bucket: str | None = None) -> None:  # noqa: ANN001
        writes_log.append((key, bucket))
        writes_by_key[key] = df

    monkeypatch.setattr(ps, "write_parquet_df", fake_write)
    monkeypatch.setattr(ps, "normalize_markets", lambda recs: bronze_stub)
    monkeypatch.setattr(ps, "normalize_price_snapshots", lambda recs: bronze_stub)
    monkeypatch.setattr(ps, "to_belief_price_snapshots", lambda df: silver_belief)
    monkeypatch.setattr(ps, "normalize_klines", lambda recs: bronze_stub)
    monkeypatch.setattr(ps, "to_crypto_candles_1m", lambda df: silver_candles)
    monkeypatch.setattr(ps, "normalize_timeline", lambda recs: bronze_stub)
    monkeypatch.setattr(ps, "to_narrative_counts", lambda df: silver_counts)

    return writes_log, writes_by_key


def test_normalize_polymarket_to_silver_writes_canonical_keys(monkeypatch) -> None:
    writes_log, _ = _patch_normalize_and_writer(monkeypatch)

    out = ps.normalize_polymarket_to_silver(
        run_date="2026-05-06",
        markets_records=[{"a": 1}],
        prices_records=[{"b": 2}],
    )

    keys = {k for k, _ in writes_log}
    assert "bronze/provider=polymarket/date=2026-05-06/markets.parquet" in keys
    assert "bronze/provider=polymarket/date=2026-05-06/prices.parquet" in keys
    assert "silver/belief_price_snapshots/date=2026-05-06/data.parquet" in keys
    assert set(out.keys()) == {
        "bronze_polymarket_markets",
        "bronze_polymarket_prices",
        "silver_belief_price_snapshots",
    }


def test_normalize_binance_to_silver_writes_canonical_keys(monkeypatch) -> None:
    writes_log, _ = _patch_normalize_and_writer(monkeypatch)

    out = ps.normalize_binance_to_silver(
        run_date="2026-05-06",
        klines_records=[{"a": 1}],
    )

    keys = {k for k, _ in writes_log}
    assert "bronze/provider=binance/date=2026-05-06/klines.parquet" in keys
    assert "silver/crypto_candles_1m/date=2026-05-06/data.parquet" in keys
    assert set(out.keys()) == {"bronze_binance_klines", "silver_crypto_candles_1m"}


def test_normalize_gdelt_to_silver_writes_canonical_keys(monkeypatch) -> None:
    writes_log, _ = _patch_normalize_and_writer(monkeypatch)

    out = ps.normalize_gdelt_to_silver(
        run_date="2026-05-06",
        timeline_records=[{"a": 1}],
    )

    keys = {k for k, _ in writes_log}
    assert "bronze/provider=gdelt/date=2026-05-06/timeline.parquet" in keys
    assert "silver/narrative_counts/date=2026-05-06/data.parquet" in keys
    assert set(out.keys()) == {"bronze_gdelt_timeline", "silver_narrative_counts"}


def test_steps_thread_bucket_into_writes(monkeypatch) -> None:
    """Bucket override must be propagated to every parquet write.

    Sample mode relies on this: a missed write would leak into the live bucket.
    """

    writes_log, _ = _patch_normalize_and_writer(monkeypatch)

    ps.normalize_polymarket_to_silver(
        run_date="2026-05-06",
        markets_records=[],
        prices_records=[],
        bucket="sample-bucket",
    )
    ps.normalize_binance_to_silver(
        run_date="2026-05-06",
        klines_records=[],
        bucket="sample-bucket",
    )
    ps.normalize_gdelt_to_silver(
        run_date="2026-05-06",
        timeline_records=[],
        bucket="sample-bucket",
    )

    assert writes_log, "expected at least one parquet write"
    assert all(b == "sample-bucket" for _, b in writes_log)


def test_steps_default_bucket_is_none(monkeypatch) -> None:
    """When bucket is omitted, writes pass ``bucket=None`` (live default routing)."""

    writes_log, _ = _patch_normalize_and_writer(monkeypatch)

    ps.normalize_binance_to_silver(run_date="2026-05-06", klines_records=[])

    assert writes_log
    assert all(b is None for _, b in writes_log)
