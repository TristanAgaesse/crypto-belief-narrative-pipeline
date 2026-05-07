from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from crypto_belief_pipeline.features.prices import build_price_features


def _candle_row(ts: datetime, close: float) -> dict:
    return {
        "timestamp": ts,
        "exchange": "binance_usdm",
        "asset": "BTC",
        "symbol": "BTCUSDT",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1.0,
        "quote_volume": close,
        "number_of_trades": 1,
    }


def test_features_only_use_past_data_and_labels_only_use_future_data() -> None:
    """Strict no-lookahead invariant.

    For an event at time t:
      - Features at t use timestamps <= t (past or current).
      - Labels at t use timestamps > t (future).

    We verify this by injecting a poison value into the past and a sentinel
    into the future and making sure features track only the past while labels
    track only the future.
    """

    base = datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC)
    rows = [
        _candle_row(base - timedelta(hours=4), 90.0),
        _candle_row(base - timedelta(hours=1), 99.0),
        _candle_row(base, 100.0),
        _candle_row(base + timedelta(hours=1), 110.0),
        _candle_row(base + timedelta(hours=4), 130.0),
        _candle_row(base + timedelta(hours=24), 150.0),
    ]
    candles = pl.DataFrame(rows)

    features = build_price_features(candles)
    event = features.filter(pl.col("event_time") == base)

    # Features (asset_close_lag_*) use only timestamps <= event_time.
    assert event["asset_close"][0] == pytest.approx(100.0)
    assert event["asset_close_lag_1h"][0] == pytest.approx(99.0)
    assert event["asset_close_lag_4h"][0] == pytest.approx(90.0)

    # Labels (future_close_*) use only timestamps > event_time.
    assert event["future_close_1h"][0] == pytest.approx(110.0)
    assert event["future_close_4h"][0] == pytest.approx(130.0)
    assert event["future_close_24h"][0] == pytest.approx(150.0)

    # Labels and features must reference different timestamps.
    assert event["future_close_1h"][0] != event["asset_close_lag_1h"][0]
    assert event["future_close_4h"][0] != event["asset_close_lag_4h"][0]


def test_future_ret_at_t_equals_close_t_plus_1h_over_close_t_minus_1() -> None:
    base = datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC)
    candles = pl.DataFrame(
        [
            _candle_row(base, 100.0),
            _candle_row(base + timedelta(hours=1), 110.0),
        ]
    )
    out = build_price_features(candles)
    event = out.filter(pl.col("event_time") == base)
    assert event["future_ret_1h"][0] == pytest.approx(110.0 / 100.0 - 1.0)
