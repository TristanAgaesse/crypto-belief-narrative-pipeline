from __future__ import annotations

import polars as pl

from crypto_belief_pipeline.transform.normalize_kalshi import (
    classify_kalshi_market_priority,
    normalize_kalshi_orderbooks,
    to_silver_kalshi_markets,
)


def test_classify_keyword_and_alias() -> None:
    cfg = {
        "keywords": ["bitcoin"],
        "asset_aliases": {"BTC": ["btc"]},
    }
    market = {"ticker": "KXBTC-1", "yes_sub_title": "Yes side", "event_ticker": "E1"}
    event = {"title": "Bitcoin price", "category": "Crypto"}
    out = classify_kalshi_market_priority(market, event, None, cfg=cfg)
    assert out["relevance_label"] == "crypto_relevant"
    assert "BTC" in out["mapped_assets"]


def test_orderbook_mid_and_spread() -> None:
    records = [
        {
            "fetched_at": "2026-05-08T12:00:00Z",
            "snapshot_time": "2026-05-08T12:00:00Z",
            "ingest_batch_id": "t1",
            "data": {
                "market_ticker": "M1",
                "orderbook": {"orderbook_fp": {"yes_dollars": [["0.60", "10"]], "no_dollars": [["0.30", "5"]]}},
            },
        }
    ]
    df = normalize_kalshi_orderbooks(records)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["best_yes_bid"] == 0.60
    assert abs(row["best_yes_ask"] - 0.70) < 1e-9
    assert abs(row["mid_probability"] - 0.65) < 1e-9


def test_to_silver_markets_empty() -> None:
    empty_mk = pl.DataFrame(
        schema={
            "market_ticker": pl.String,
            "event_ticker": pl.String,
            "series_ticker": pl.String,
            "status": pl.String,
            "market_type": pl.String,
            "fetched_at": pl.Datetime,
            "snapshot_time": pl.Datetime,
            "yes_bid": pl.Float64,
            "yes_ask": pl.Float64,
            "last_price": pl.Float64,
            "volume_fp": pl.String,
            "volume_24h_fp": pl.String,
            "open_interest_fp": pl.String,
            "raw_json": pl.String,
            "ingested_at": pl.Datetime,
        }
    )
    out = to_silver_kalshi_markets(empty_mk, pl.DataFrame(), pl.DataFrame(), cfg={})
    assert out.height == 0
