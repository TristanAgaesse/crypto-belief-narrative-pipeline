import polars as pl
import pytest

from crypto_belief_pipeline.sample_data import load_sample_jsonl
from crypto_belief_pipeline.transform.normalize_polymarket import (
    normalize_markets,
    normalize_price_snapshots,
    to_belief_price_snapshots,
)


def test_normalize_markets_columns_and_rows() -> None:
    records = load_sample_jsonl("polymarket_markets_sample.jsonl")
    df = normalize_markets(records)
    assert df.height == 3
    assert set(df.columns) == {
        "market_id",
        "question",
        "slug",
        "category",
        "active",
        "closed",
        "end_date",
        "liquidity",
        "volume",
        "raw_json",
        "ingested_at",
    }
    assert df.schema["end_date"] == pl.Datetime


def test_normalize_price_snapshots_spread() -> None:
    records = [
        {
            "timestamp": "2026-05-06T10:00:00Z",
            "market_id": "pm_btc_reserve_001",
            "outcome": "Yes",
            "price": 0.32,
            "best_bid": 0.31,
            "best_ask": 0.33,
            "liquidity": 1.0,
            "volume": 2.0,
            "raw": {"source": "sample"},
        }
    ]
    df = normalize_price_snapshots(records)
    assert df.height == 1
    assert df["spread"][0] == pytest.approx(0.02)


def test_to_belief_price_snapshots_sets_platform() -> None:
    bronze_records = load_sample_jsonl("polymarket_prices_sample.jsonl")
    bronze = normalize_price_snapshots(bronze_records)
    silver = to_belief_price_snapshots(bronze)
    assert silver.height == 9
    assert silver["platform"].unique().to_list() == ["polymarket"]
