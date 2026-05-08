"""Kalshi collector scope aligned to docs/alpha_hypothesis.md (BTC/ETH/SOL)."""

from __future__ import annotations

from crypto_belief_pipeline.collectors.kalshi import market_matches_hypothesis_scope


def _cfg() -> dict:
    return {
        "keywords": ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "crypto"],
        "asset_aliases": {
            "BTC": ["btc", "bitcoin"],
            "ETH": ["eth", "ethereum"],
            "SOL": ["sol", "solana"],
        },
    }


def test_scope_positive_on_btc_title() -> None:
    m = {"ticker": "KXFOO-99", "yes_sub_title": "Bitcoin above 100k", "event_ticker": "EVT"}
    assert market_matches_hypothesis_scope(m, _cfg()) is True


def test_scope_negative_on_unrelated() -> None:
    m = {"ticker": "KXWEATHER-1", "yes_sub_title": "Rain in Seattle", "event_ticker": "WX"}
    assert market_matches_hypothesis_scope(m, _cfg()) is False


def test_scope_all_when_no_keywords_or_aliases() -> None:
    m = {"ticker": "KX-1", "yes_sub_title": "Anything"}
    assert market_matches_hypothesis_scope(m, {}) is True
    assert market_matches_hypothesis_scope(m, {"keywords": [], "asset_aliases": {}}) is True
