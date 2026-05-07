from __future__ import annotations

from datetime import date

import crypto_belief_pipeline.orchestration.raw_inputs_from_lake as ri


def test_resolve_polymarket_prefers_latest_microbatch(monkeypatch) -> None:
    keys = [
        "raw/provider=polymarket/date=2026-05-07/hour=10/batch_id=20260507T100000Z_markets.jsonl",
        "raw/provider=polymarket/date=2026-05-07/hour=10/batch_id=20260507T100000Z_prices.jsonl",
        "raw/provider=polymarket/date=2026-05-07/hour=11/batch_id=20260507T110000Z_markets.jsonl",
        "raw/provider=polymarket/date=2026-05-07/hour=11/batch_id=20260507T110000Z_prices.jsonl",
    ]

    monkeypatch.setattr(ri, "list_jsonl_keys_under", lambda prefix, bucket=None: list(keys))
    monkeypatch.setattr(ri, "_candidate_buckets", lambda: ["lake"])

    d, bucket = ri.resolve_raw_polymarket_for_partition(date(2026, 5, 7))
    assert bucket == "lake"
    assert d["raw_polymarket_markets"].endswith("20260507T110000Z_markets.jsonl")
    assert d["raw_polymarket_prices"].endswith("20260507T110000Z_prices.jsonl")
    assert d["source_batch_id"] == "20260507T110000Z"


def test_resolve_polymarket_live_pair(monkeypatch) -> None:
    keys = [
        "raw/provider=polymarket/date=2026-05-07/live_markets.jsonl",
        "raw/provider=polymarket/date=2026-05-07/live_prices.jsonl",
    ]
    monkeypatch.setattr(ri, "list_jsonl_keys_under", lambda prefix, bucket=None: list(keys))
    monkeypatch.setattr(ri, "_candidate_buckets", lambda: ["lake"])

    d, bucket = ri.resolve_raw_polymarket_for_partition(date(2026, 5, 7))
    assert bucket == "lake"
    assert d["raw_polymarket_markets"].endswith("live_markets.jsonl")
    assert d["source_batch_id"] == ""


def test_resolve_binance_micro(monkeypatch) -> None:
    keys = [
        "raw/provider=binance/date=2026-05-07/hour=10/batch_id=20260507T100000Z.jsonl",
        "raw/provider=binance/date=2026-05-07/hour=11/batch_id=20260507T110000Z.jsonl",
    ]
    monkeypatch.setattr(ri, "list_jsonl_keys_under", lambda prefix, bucket=None: list(keys))
    monkeypatch.setattr(ri, "_candidate_buckets", lambda: ["lake"])

    d, bucket = ri.resolve_raw_binance_for_partition(date(2026, 5, 7))
    assert bucket == "lake"
    assert d["raw_binance_klines"].endswith("20260507T110000Z.jsonl")
    assert d["source_batch_id"] == "20260507T110000Z"


def test_resolve_gdelt_fallback_timeline(monkeypatch) -> None:
    monkeypatch.setattr(ri, "list_jsonl_keys_under", lambda prefix, bucket=None: [])
    monkeypatch.setattr(ri, "_candidate_buckets", lambda: ["lake"])

    d, bucket = ri.resolve_raw_gdelt_for_partition(date(2026, 5, 7))
    assert bucket == "lake"
    assert d["raw_gdelt_timeline"] == "raw/provider=gdelt/date=2026-05-07/live_timeline.jsonl"
