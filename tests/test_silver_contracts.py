"""Silver contract enforcement at production boundaries.

Contracts in :mod:`crypto_belief_pipeline.contracts` describe the columns
downstream stages (gold join, DQ, issues) depend on. We validate each silver
output *before* writing so a bad transform fails fast instead of poisoning the
lake.
"""

from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest

import crypto_belief_pipeline.transform.pipeline_steps as ps
from crypto_belief_pipeline.contracts import (
    SILVER_BELIEF_PRICE_SNAPSHOTS,
    SILVER_CRYPTO_CANDLES_1M,
)


def _patch_writer_only(monkeypatch) -> list[tuple[str, str | None]]:
    writes: list[tuple[str, str | None]] = []

    def fake_write(df, key: str, bucket: str | None = None) -> None:  # noqa: ANN001
        writes.append((key, bucket))

    monkeypatch.setattr(ps, "write_parquet_df", fake_write)
    return writes


def test_polymarket_step_raises_when_silver_missing_required_columns(monkeypatch) -> None:
    """Silver belief_price_snapshots without ``platform`` must fail validation."""

    _patch_writer_only(monkeypatch)
    empty = pl.DataFrame({"x": [1]})
    monkeypatch.setattr(ps, "normalize_markets", lambda recs: empty)
    monkeypatch.setattr(ps, "normalize_price_snapshots", lambda recs: empty)
    # Silver projection that drops the contract-required ``platform`` column.
    monkeypatch.setattr(
        ps,
        "to_belief_price_snapshots",
        lambda df: pl.DataFrame(
            {
                "timestamp": [],
                "market_id": [],
                "outcome": [],
                "price": [],
            }
        ),
    )

    with pytest.raises(ValueError, match="silver_belief_price_snapshots: missing required columns"):
        ps.normalize_polymarket_to_silver(
            run_date="2026-05-06",
            markets_records=[],
            prices_records=[],
        )


def test_binance_step_raises_when_silver_missing_required_columns(monkeypatch) -> None:
    """Silver crypto_candles_1m without ``close`` must fail validation."""

    _patch_writer_only(monkeypatch)
    monkeypatch.setattr(ps, "normalize_klines", lambda recs: pl.DataFrame({"x": [1]}))
    monkeypatch.setattr(
        ps,
        "to_crypto_candles_1m",
        lambda df: pl.DataFrame(
            {
                "timestamp": [],
                "exchange": [],
                "asset": [],
            }
        ),
    )

    with pytest.raises(ValueError, match="silver_crypto_candles_1m: missing required columns"):
        ps.normalize_binance_to_silver(run_date="2026-05-06", klines_records=[])


def test_gdelt_step_raises_when_silver_missing_required_columns(monkeypatch) -> None:
    """Silver narrative_counts without ``mention_volume`` must fail validation."""

    _patch_writer_only(monkeypatch)
    monkeypatch.setattr(ps, "normalize_timeline", lambda recs: pl.DataFrame({"x": [1]}))
    monkeypatch.setattr(
        ps,
        "to_narrative_counts",
        lambda df: pl.DataFrame(
            {
                "timestamp": [],
                "narrative": [],
            }
        ),
    )

    with pytest.raises(ValueError, match="silver_narrative_counts: missing required columns"):
        ps.normalize_gdelt_to_silver(run_date="2026-05-06", timeline_records=[])


def test_silver_validation_runs_before_write(monkeypatch) -> None:
    """Failing validation must short-circuit before the silver parquet hits the lake."""

    writes = _patch_writer_only(monkeypatch)
    monkeypatch.setattr(ps, "normalize_klines", lambda recs: pl.DataFrame({"x": [1]}))
    monkeypatch.setattr(
        ps,
        "to_crypto_candles_1m",
        lambda df: pl.DataFrame({"timestamp": [], "exchange": [], "asset": []}),
    )

    with pytest.raises(ValueError):
        ps.normalize_binance_to_silver(run_date="2026-05-06", klines_records=[])

    silver_writes = [k for k, _ in writes if k.startswith("silver/")]
    assert silver_writes == [], "silver parquet must not be written when validation fails"


def test_contract_rejects_wrong_dtype() -> None:
    df = pl.DataFrame(
        {
            "timestamp": ["2026-05-06T00:00:00Z"],
            "platform": ["polymarket"],
            "market_id": ["m1"],
            "outcome": ["Yes"],
            "price": [0.5],
        }
    )

    with pytest.raises(ValueError, match="invalid column dtypes"):
        SILVER_BELIEF_PRICE_SNAPSHOTS.validate(df)


def test_contract_rejects_null_required_key() -> None:
    df = pl.DataFrame(
        {
            "timestamp": [datetime(2026, 5, 6, 0, 0)],
            "exchange": ["binance_usdm"],
            "asset": [None],
            "close": [64000.0],
        },
        schema={
            "timestamp": pl.Datetime,
            "exchange": pl.String,
            "asset": pl.String,
            "close": pl.Float64,
        },
    )

    with pytest.raises(ValueError, match="non-null constraint failed"):
        SILVER_CRYPTO_CANDLES_1M.validate(df)


def test_contract_rejects_duplicate_unique_key() -> None:
    ts = datetime(2026, 5, 6, 0, 0)
    df = pl.DataFrame(
        {
            "timestamp": [ts, ts],
            "exchange": ["binance_usdm", "binance_usdm"],
            "asset": ["BTC", "BTC"],
            "close": [64000.0, 64001.0],
        }
    )

    with pytest.raises(ValueError, match="unique key"):
        SILVER_CRYPTO_CANDLES_1M.validate(df)


def test_contract_rejects_invalid_enum_value() -> None:
    df = pl.DataFrame(
        {
            "timestamp": [datetime(2026, 5, 6, 0, 0)],
            "exchange": ["coinbase"],
            "asset": ["BTC"],
            "close": [64000.0],
        }
    )

    with pytest.raises(ValueError, match="invalid values"):
        SILVER_CRYPTO_CANDLES_1M.validate(df)
