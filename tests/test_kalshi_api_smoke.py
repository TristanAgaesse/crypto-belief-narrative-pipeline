"""Optional live call to Kalshi public Trade API (off in CI).

Set ``KALSHI_API_SMOKE=1`` and run with network, e.g.::

    KALSHI_API_SMOKE=1 pytest tests/test_kalshi_api_smoke.py -q
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("httpx")

from crypto_belief_pipeline.collectors.kalshi import collect_kalshi_raw, fetch_markets


@pytest.mark.skipif(
    os.environ.get("KALSHI_API_SMOKE") != "1",
    reason="Set KALSHI_API_SMOKE=1 to hit the real Kalshi API",
)
def test_kalshi_public_markets_roundtrip() -> None:
    cfg = {
        "markets_status": "open",
        "markets_page_limit": 5,
        "markets_max_pages": 3,
        "events_page_limit": 5,
        "events_max_pages": 3,
        "trades_max_rows": 50,
        "trades_max_pages": 5,
        "max_series_fetch": 3,
        "max_markets_orderbook": 2,
        "max_markets_candles": 1,
        "candle_period_minutes": 60,
        "keywords": ["btc"],
        "asset_aliases": {"BTC": ["btc", "bitcoin"]},
        "scope_markets_to_hypothesis": False,
        "trades_scope_to_filtered_markets": False,
        "disabled_endpoints": [],
    }
    t0 = time.perf_counter()
    rows, err = fetch_markets(cfg)
    dt = time.perf_counter() - t0
    assert dt < 120.0, f"markets fetch took {dt:.1f}s (check timeouts / network)"
    assert err is None
    assert isinstance(rows, list)


@pytest.mark.skipif(
    os.environ.get("KALSHI_API_SMOKE") != "1",
    reason="Set KALSHI_API_SMOKE=1 to hit the real Kalshi API",
)
def test_collect_kalshi_raw_completes_under_wall_clock() -> None:
    """End-to-end collector with tight limits; should finish, not hang."""
    cfg = {
        "markets_status": "open",
        "markets_page_limit": 20,
        "markets_max_pages": 5,
        "events_page_limit": 20,
        "events_max_pages": 5,
        "trades_max_rows": 100,
        "trades_max_pages": 10,
        "max_series_fetch": 5,
        "max_markets_orderbook": 3,
        "max_markets_candles": 2,
        "candle_period_minutes": 60,
        "keywords": ["bitcoin"],
        "asset_aliases": {"BTC": ["btc", "bitcoin"]},
        "scope_markets_to_hypothesis": False,
        "trades_scope_to_filtered_markets": False,
        "disabled_endpoints": [],
    }
    now = datetime.now(UTC).replace(microsecond=0)
    start = now - timedelta(hours=2)
    t0 = time.perf_counter()
    payloads, meta = collect_kalshi_raw(
        start_time=start,
        end_time=now,
        snapshot_time=now,
        ingest_batch_id="smoke_test_batch",
        cfg=cfg,
    )
    dt = time.perf_counter() - t0
    assert dt < 300.0, f"collect_kalshi_raw took {dt:.1f}s — possible stall or very slow API"
    assert set(payloads.keys()) == {
        "markets",
        "events",
        "series",
        "trades",
        "orderbooks",
        "candlesticks",
    }
    assert meta.get("markets_rows", 0) >= 0
