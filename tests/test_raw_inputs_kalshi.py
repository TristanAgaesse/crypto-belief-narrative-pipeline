from __future__ import annotations

from datetime import UTC, datetime

import crypto_belief_pipeline.orchestration.raw_inputs_from_lake as ri


def test_list_kalshi_microbatch_window(monkeypatch) -> None:
    keys = [
        "raw/provider=kalshi/date=2026-05-07/hour=11/batch_id=20260507T110000Z_markets.jsonl",
        "raw/provider=kalshi/date=2026-05-07/hour=11/batch_id=20260507T110000Z_events.jsonl",
        "raw/provider=kalshi/date=2026-05-07/hour=11/batch_id=20260507T110000Z_series.jsonl",
        "raw/provider=kalshi/date=2026-05-07/hour=11/batch_id=20260507T110000Z_trades.jsonl",
        "raw/provider=kalshi/date=2026-05-07/hour=11/batch_id=20260507T110000Z_orderbooks.jsonl",
        "raw/provider=kalshi/date=2026-05-07/hour=11/batch_id=20260507T110000Z_candlesticks.jsonl",
    ]
    monkeypatch.setattr(ri, "list_jsonl_keys_under", lambda prefix, bucket=None: list(keys))
    monkeypatch.setattr(ri, "_candidate_buckets", lambda: ["lake"])

    out, bucket = ri.list_raw_kalshi_for_partition_window(
        partition_start=datetime(2026, 5, 7, 11, 0, 0, tzinfo=UTC),
        partition_end=datetime(2026, 5, 7, 11, 1, 0, tzinfo=UTC),
    )
    assert bucket == "lake"
    assert len(out) == 1
    assert out[0]["source_batch_id"] == "20260507T110000Z"
    assert out[0]["raw_kalshi_markets"].endswith("_markets.jsonl")


def test_batch_id_strips_kalshi_suffix() -> None:
    k = "raw/x/batch_id=20260507T110000Z_orderbooks.jsonl"
    assert ri._batch_id_from_micro_key(k) == "20260507T110000Z"
