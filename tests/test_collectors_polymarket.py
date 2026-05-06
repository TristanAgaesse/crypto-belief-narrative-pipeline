from __future__ import annotations

import json
from datetime import UTC, datetime

from crypto_belief_pipeline.collectors import polymarket as pm


def test_filter_markets_by_keywords_matches_question_slug_tags_case_insensitively() -> None:
    markets = [
        {"question": "Will Bitcoin hit $100k?", "slug": "btc-100k", "tags": []},
        {
            "question": "Will Apple ship a foldable?",
            "slug": "apple-fold",
            "tags": [{"label": "Technology"}],
        },
        {
            "question": "Election outcome",
            "slug": "elec-2026",
            "tags": [{"label": "Politics"}, {"label": "ETF"}],
        },
        {"question": "Sports prediction", "slug": "nba-finals", "tags": ["sports"]},
    ]

    out = pm.filter_markets_by_keywords(markets, ["bitcoin", "etf"])
    slugs = {m["slug"] for m in out}
    assert slugs == {"btc-100k", "elec-2026"}


def test_filter_markets_by_keywords_empty_keywords_returns_all() -> None:
    markets = [{"question": "anything", "slug": "x"}]
    assert pm.filter_markets_by_keywords(markets, []) == markets


def test_to_raw_market_record_handles_missing_fields_and_id_fallback() -> None:
    market = {
        "conditionId": "0xabc",
        "question": "Q?",
        "slug": "q-slug",
        "active": "true",
        "closed": False,
        "endDate": "2026-12-31T23:59:59Z",
        "liquidityNum": "1234.5",
        "volumeNum": "9876",
        "tags": [{"label": "crypto"}, {"label": "BTC"}],
    }
    out = pm.to_raw_market_record(market)
    assert out["market_id"] == "0xabc"
    assert out["question"] == "Q?"
    assert out["slug"] == "q-slug"
    assert out["category"] == "crypto"
    assert out["active"] is True
    assert out["closed"] is False
    assert out["end_date"] == "2026-12-31T23:59:59Z"
    assert out["liquidity"] == 1234.5
    assert out["volume"] == 9876.0
    assert out["raw"] is market


def test_to_raw_market_record_falls_back_to_slug_when_no_id() -> None:
    market = {"slug": "only-slug", "question": "Q?", "active": True}
    out = pm.to_raw_market_record(market)
    assert out["market_id"] == "only-slug"
    assert out["liquidity"] == 0.0
    assert out["volume"] == 0.0


def test_extract_price_snapshots_outcomes_as_lists() -> None:
    markets = [
        {
            "id": "m1",
            "outcomes": ["Yes", "No"],
            "outcomePrices": [0.42, 0.58],
            "liquidity": 100.0,
            "volume": 200.0,
        }
    ]
    snap = datetime(2026, 5, 6, 12, 0, 0, tzinfo=UTC)
    out = pm.extract_price_snapshots(markets, snapshot_time=snap)
    assert len(out) == 2
    yes = next(r for r in out if r["outcome"] == "Yes")
    no = next(r for r in out if r["outcome"] == "No")
    assert yes["price"] == 0.42
    assert no["price"] == 0.58
    assert yes["timestamp"] == "2026-05-06T12:00:00Z"
    assert yes["best_bid"] is None and yes["best_ask"] is None
    assert yes["liquidity"] == 100.0 and yes["volume"] == 200.0
    assert yes["market_id"] == "m1"


def test_extract_price_snapshots_outcomes_as_json_strings() -> None:
    markets = [
        {
            "id": "m2",
            "outcomes": json.dumps(["Yes", "No"]),
            "outcomePrices": json.dumps(["0.30", "0.70"]),
        }
    ]
    out = pm.extract_price_snapshots(markets)
    assert len(out) == 2
    prices = {r["outcome"]: r["price"] for r in out}
    assert prices == {"Yes": 0.30, "No": 0.70}


def test_extract_price_snapshots_market_level_best_bid_ask_on_yes() -> None:
    markets = [
        {
            "id": "m-book",
            "outcomes": ["Yes", "No"],
            "outcomePrices": [0.56, 0.44],
            "bestBid": 0.55,
            "bestAsk": 0.57,
        }
    ]
    out = pm.extract_price_snapshots(markets)
    yes = next(r for r in out if r["outcome"] == "Yes")
    no = next(r for r in out if r["outcome"] == "No")
    assert yes["best_bid"] == 0.55 and yes["best_ask"] == 0.57
    assert no["best_bid"] is None and no["best_ask"] is None


def test_extract_price_snapshots_tokens_shape_includes_bid_ask() -> None:
    markets = [
        {
            "conditionId": "cond-1",
            "tokens": [
                {"outcome": "Yes", "price": 0.42, "bestBid": 0.41, "bestAsk": 0.43},
                {"outcome": "No", "price": 0.58, "bestBid": 0.57, "bestAsk": 0.59},
            ],
        }
    ]
    out = pm.extract_price_snapshots(markets)
    assert len(out) == 2
    yes = next(r for r in out if r["outcome"] == "Yes")
    assert yes["best_bid"] == 0.41 and yes["best_ask"] == 0.43
    assert yes["market_id"] == "cond-1"


def test_extract_price_snapshots_skips_markets_with_no_price() -> None:
    markets = [
        {"id": "m3", "outcomes": ["Yes"], "outcomePrices": []},
        {"id": "m4"},
    ]
    out = pm.extract_price_snapshots(markets)
    assert out == []


def test_collect_polymarket_raw_filters_and_maps(monkeypatch) -> None:
    def fake_fetch(limit: int, active: bool, closed: bool) -> list[dict]:
        return [
            {
                "id": "x",
                "question": "Will Bitcoin moon?",
                "slug": "btc-moon",
                "outcomes": ["Yes", "No"],
                "outcomePrices": [0.7, 0.3],
                "liquidity": 10.0,
                "volume": 20.0,
                "active": True,
                "closed": False,
            },
            {
                "id": "y",
                "question": "Will Apple ship a foldable?",
                "slug": "apple-fold",
            },
        ]

    monkeypatch.setattr(pm, "fetch_gamma_markets", fake_fetch)
    markets, prices = pm.collect_polymarket_raw(limit=5, keywords=["bitcoin"])
    assert len(markets) == 1
    assert markets[0]["market_id"] == "x"
    assert {r["outcome"] for r in prices} == {"Yes", "No"}
