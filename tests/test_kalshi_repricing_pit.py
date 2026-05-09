"""Point-in-time tests for Kalshi repricing trade volume (no lookahead)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from crypto_belief_pipeline.features.kalshi_repricing import build_event_repricing_features


def _snap_row(fetched_at: datetime, mid: float, spread: float = 0.01) -> dict:
    return {
        "fetched_at": fetched_at,
        "market_ticker": "KXBTC-99",
        "event_ticker": "EVT1",
        "mid_probability": mid,
        "spread": spread,
        "last_price": None,
    }


def test_trade_confirm_score_excludes_trades_after_as_of() -> None:
    """Trades after snapshot as_of must not inflate cumulative volume."""

    base = datetime(2026, 5, 6, 12, 0, 0, tzinfo=UTC)
    snapshots = pl.DataFrame(
        [
            _snap_row(base - timedelta(minutes=10), 0.40),
            _snap_row(base, 0.50),
        ]
    )
    trades = pl.DataFrame(
        [
            {
                "executed_at": base - timedelta(minutes=30),
                "market_ticker": "KXBTC-99",
                "trade_id": "t1",
                "taker_side": "yes",
                "yes_price": 0.4,
                "no_price": 0.6,
                "size_fp": "10",
            },
            {
                "executed_at": base + timedelta(minutes=30),
                "market_ticker": "KXBTC-99",
                "trade_id": "t2",
                "taker_side": "yes",
                "yes_price": 0.5,
                "no_price": 0.5,
                "size_fp": "1000",
            },
        ]
    )
    markets = pl.DataFrame(
        {
            "market_ticker": ["KXBTC-99"],
            "event_ticker": ["EVT1"],
            "mapped_assets": ["BTC"],
        }
    )

    out = build_event_repricing_features(snapshots, trades, markets)
    row_at_base = out.filter(pl.col("as_of") == base)
    assert row_at_base.height == 1
    # Only 10 contracts traded before `base`; post-base trade must not count.
    assert float(row_at_base["trade_confirm_score"][0]) == pytest.approx(
        10.0 * abs(float(row_at_base["delta_5m"][0]))
    )


def test_trade_confirm_score_includes_trades_at_or_before_as_of() -> None:
    base = datetime(2026, 5, 6, 12, 0, 0, tzinfo=UTC)
    snapshots = pl.DataFrame(
        [
            _snap_row(base - timedelta(minutes=10), 0.40),
            _snap_row(base, 0.50),
        ]
    )
    trades = pl.DataFrame(
        [
            {
                "executed_at": base,
                "market_ticker": "KXBTC-99",
                "trade_id": "t1",
                "taker_side": "yes",
                "yes_price": 0.5,
                "no_price": 0.5,
                "size_fp": "7",
            },
        ]
    )
    markets = pl.DataFrame(
        {
            "market_ticker": ["KXBTC-99"],
            "event_ticker": ["EVT1"],
            "mapped_assets": ["BTC"],
        }
    )
    out = build_event_repricing_features(snapshots, trades, markets)
    row = out.filter(pl.col("as_of") == base)
    d5 = float(row["delta_5m"][0])
    assert d5 == pytest.approx(0.1)  # 0.5 now vs 0.4 at last snapshot at or before base-5m
    assert float(row["trade_confirm_score"][0]) == pytest.approx(7.0 * abs(d5))
