from typing import Any

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
        "load_timestamp",
    }
    assert df.schema["end_date"] == pl.Datetime


def test_normalize_markets_accepts_null_end_date() -> None:
    records: list[dict[str, Any]] = [
        {
            "market_id": "m1",
            "question": "Q?",
            "slug": "q",
            "category": None,
            "active": True,
            "closed": False,
            "end_date": None,
            "liquidity": 1.0,
            "volume": 2.0,
            "raw": {},
        }
    ]
    df = normalize_markets(records)
    assert df.height == 1
    assert df["end_date"][0] is None


def test_normalize_price_snapshots_null_spread_when_book_missing() -> None:
    records = [
        {
            "timestamp": "2026-05-06T10:00:00Z",
            "market_id": "m",
            "outcome": "No",
            "price": 0.4,
            "best_bid": None,
            "best_ask": None,
            "liquidity": 1.0,
            "volume": 2.0,
            "raw": {},
        }
    ]
    df = normalize_price_snapshots(records)
    assert df.height == 1
    assert df["spread"][0] is None


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


def test_normalize_price_snapshots_many_leading_null_prices_then_float() -> None:
    """Polars default infer_schema_length=100 must not infer Null for price then reject f64."""

    base = {
        "timestamp": "2026-05-06T10:00:00Z",
        "market_id": "m",
        "outcome": "Yes",
        "best_bid": None,
        "best_ask": None,
        "liquidity": 1.0,
        "volume": 2.0,
        "raw": {},
    }
    records = [{**base, "price": None} for _ in range(150)] + [
        {
            **base,
            "timestamp": "2026-05-06T10:01:00Z",
            "price": 0.01,
            "best_bid": 0.0,
            "best_ask": 0.02,
        }
    ]
    df = normalize_price_snapshots(records)
    # Rows share (market_id, outcome); same timestamp dedupes to one null-price row
    # plus the later timestamp with a float price.
    assert df.height == 2
    assert df.filter(pl.col("price") == 0.01).height == 1


def test_normalize_price_snapshots_handles_mixed_int_and_float_numeric_fields() -> None:
    records = [
        {
            "timestamp": "2026-05-06T10:00:00Z",
            "market_id": "pm_btc_reserve_001",
            "outcome": "Yes",
            "price": 0,
            "best_bid": 0,
            "best_ask": 0,
            "liquidity": 10,
            "volume": 20,
            "raw": {"source": "sample"},
        },
        {
            "timestamp": "2026-05-06T10:01:00Z",
            "market_id": "pm_btc_reserve_001",
            "outcome": "Yes",
            "price": 0.04,
            "best_bid": 0.03,
            "best_ask": 0.05,
            "liquidity": 10.5,
            "volume": 20.25,
            "raw": {"source": "sample"},
        },
    ]
    df = normalize_price_snapshots(records)
    assert df.height == 2
    assert df.schema["price"] == pl.Float64
    assert df.schema["best_bid"] == pl.Float64
    assert df.schema["best_ask"] == pl.Float64
    assert df["spread"].max() == pytest.approx(0.02)


def test_normalize_price_snapshots_treats_boolean_numeric_fields_as_null() -> None:
    records = [
        {
            "timestamp": "2026-05-06T10:00:00Z",
            "market_id": "pm_btc_reserve_001",
            "outcome": "Yes",
            "price": True,
            "best_bid": False,
            "best_ask": True,
            "liquidity": False,
            "volume": True,
            "raw": {"source": "sample"},
        }
    ]
    df = normalize_price_snapshots(records)
    assert df.height == 1
    assert df["price"][0] is None
    assert df["best_bid"][0] is None
    assert df["best_ask"][0] is None
    assert df["liquidity"][0] is None
    assert df["volume"][0] is None
    assert df["spread"][0] is None


def test_to_belief_price_snapshots_sets_platform() -> None:
    bronze_records = load_sample_jsonl("polymarket_prices_sample.jsonl")
    bronze = normalize_price_snapshots(bronze_records)
    silver = to_belief_price_snapshots(bronze)
    assert silver.height == 9
    assert silver["platform"].unique().to_list() == ["polymarket"]
