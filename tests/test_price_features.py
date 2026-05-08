from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from crypto_belief_pipeline.features.prices import build_price_features


def _candle_row(ts: datetime, asset: str, close: float, symbol: str) -> dict:
    return {
        "timestamp": ts,
        "exchange": "binance_usdm",
        "asset": asset,
        "symbol": symbol,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1.0,
        "quote_volume": close,
        "number_of_trades": 1,
    }


def _build_candles(closes_by_hour: dict[int, float]) -> pl.DataFrame:
    base = datetime(2026, 5, 6, 0, 0, 0, tzinfo=UTC)
    rows = [
        _candle_row(base + timedelta(hours=h), "BTC", c, "BTCUSDT")
        for h, c in closes_by_hour.items()
    ]
    return pl.DataFrame(rows)


def test_future_ret_1h_uses_t_plus_1h_not_t_minus_1h() -> None:
    candles = _build_candles({9: 99.0, 10: 100.0, 11: 110.0})
    out = build_price_features(candles)
    event = out.filter(pl.col("event_time") == datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC))
    assert event["future_ret_1h"][0] == pytest.approx(110.0 / 100.0 - 1.0)


def test_past_ret_1h_uses_t_minus_1h() -> None:
    candles = _build_candles({9: 99.0, 10: 100.0, 11: 110.0})
    out = build_price_features(candles)
    event = out.filter(pl.col("event_time") == datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC))
    assert event["asset_ret_past_1h"][0] == pytest.approx(100.0 / 99.0 - 1.0)


def test_missing_future_timestamp_yields_null_label() -> None:
    candles = _build_candles({10: 100.0})
    out = build_price_features(candles)
    only = out.filter(pl.col("event_time") == datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC))
    assert only["future_ret_1h"][0] is None
    assert only["future_ret_4h"][0] is None
    assert only["future_ret_24h"][0] is None


def test_future_ret_uses_bounded_forward_asof_when_target_slightly_late() -> None:
    base = datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC)
    candles = pl.DataFrame(
        [
            _candle_row(base, "BTC", 100.0, "BTCUSDT"),
            _candle_row(base + timedelta(hours=1, seconds=30), "BTC", 111.0, "BTCUSDT"),
        ]
    )

    out = build_price_features(candles)
    event = out.filter(pl.col("event_time") == base)
    assert event["future_ret_1h"][0] == pytest.approx(111.0 / 100.0 - 1.0)
    assert event["future_close_1h_source_time"][0] == base + timedelta(hours=1, seconds=30)


def test_future_ret_stays_null_when_asof_target_outside_tolerance() -> None:
    base = datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC)
    candles = pl.DataFrame(
        [
            _candle_row(base, "BTC", 100.0, "BTCUSDT"),
            _candle_row(base + timedelta(hours=1, minutes=5), "BTC", 111.0, "BTCUSDT"),
        ]
    )

    out = build_price_features(candles)
    event = out.filter(pl.col("event_time") == base)
    assert event["future_ret_1h"][0] is None


def test_future_ret_24h_uses_t_plus_24h() -> None:
    candles = _build_candles({10: 100.0, 34: 130.0})  # 34 = 10 + 24
    out = build_price_features(candles)
    event = out.filter(pl.col("event_time") == datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC))
    assert event["future_ret_24h"][0] == pytest.approx(130.0 / 100.0 - 1.0)


def test_price_features_include_pit_audit_columns() -> None:
    candles = _build_candles({9: 99.0, 10: 100.0, 11: 110.0, 14: 130.0, 34: 150.0})
    out = build_price_features(candles)
    event_time = datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC)
    event = out.filter(pl.col("event_time") == event_time)

    assert event["asset_close_lag_1h_source_time"][0] == datetime(2026, 5, 6, 9, 0, 0, tzinfo=UTC)
    assert event["future_close_1h_source_time"][0] == datetime(2026, 5, 6, 11, 0, 0, tzinfo=UTC)
    assert event["future_close_1h_age"][0] == timedelta(hours=1)
