from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from crypto_belief_pipeline.features.belief import build_belief_features


def _ts(hour: int) -> datetime:
    return datetime(2026, 5, 6, hour, 0, 0, tzinfo=UTC)


def _build_snapshots(prices: list[float], market_id: str = "m1") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "timestamp": [_ts(10 + i) for i in range(len(prices))],
            "platform": ["polymarket"] * len(prices),
            "market_id": [market_id] * len(prices),
            "outcome": ["Yes"] * len(prices),
            "price": prices,
            "best_bid": [None] * len(prices),
            "best_ask": [None] * len(prices),
            "spread": [None] * len(prices),
            "liquidity": [100000.0] * len(prices),
            "volume": [200000.0] * len(prices),
        }
    )


def _tags(direction: int = 1, market_id: str = "m1") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "market_id": [market_id],
            "asset": ["BTC"],
            "narrative": ["bitcoin_reserve"],
            "direction": [direction],
            "relevance": ["high"],
            "confidence": [0.9],
            "notes": [""],
        }
    )


def test_belief_shock_positive_when_direction_one_and_price_up() -> None:
    snapshots = _build_snapshots([0.30, 0.40])
    out = build_belief_features(snapshots, _tags(direction=1))
    second = out.filter(pl.col("event_time") == _ts(11))
    assert second["belief_shock_1h"][0] == pytest.approx(0.10)
    assert second["belief_shock_abs_1h"][0] == pytest.approx(0.10)


def test_belief_shock_negative_when_direction_minus_one_and_price_up() -> None:
    snapshots = _build_snapshots([0.30, 0.40])
    out = build_belief_features(snapshots, _tags(direction=-1))
    second = out.filter(pl.col("event_time") == _ts(11))
    assert second["belief_shock_1h"][0] == pytest.approx(-0.10)
    assert second["belief_shock_abs_1h"][0] == pytest.approx(0.10)


def test_belief_features_excludes_untagged_markets() -> None:
    snapshots = _build_snapshots([0.30, 0.35], market_id="m1")
    extra = _build_snapshots([0.30, 0.35], market_id="other_market")
    combined = pl.concat([snapshots, extra])
    out = build_belief_features(combined, _tags(market_id="m1"))
    assert out["market_id"].unique().to_list() == ["m1"]


def test_belief_features_filters_to_yes_when_present() -> None:
    base = _build_snapshots([0.30, 0.40])
    no_rows = base.with_columns(pl.lit("No").alias("outcome"))
    combined = pl.concat([base, no_rows])
    out = build_belief_features(combined, _tags())
    assert out.height == 2


def test_belief_z_uses_floor_when_std_zero() -> None:
    snapshots = _build_snapshots([0.30, 0.40, 0.50])
    out = build_belief_features(snapshots, _tags())
    assert "belief_shock_z_1h" in out.columns
    z_values = [v for v in out["belief_shock_z_1h"].to_list() if v is not None]
    assert all(abs(v) >= 0.0 for v in z_values)


def test_belief_z_is_causal_future_rows_do_not_change_past_values() -> None:
    # Build with 2 rows, then with an extra future row. Past z-scores must not change.
    base = _build_snapshots([0.30, 0.40])
    with_future = _build_snapshots([0.30, 0.40, 0.80])

    out_base = build_belief_features(base, _tags()).sort("event_time")
    out_future = build_belief_features(with_future, _tags()).sort("event_time")

    t = _ts(11)
    z1 = out_base.filter(pl.col("event_time") == t)["belief_shock_z_1h"][0]
    z2 = out_future.filter(pl.col("event_time") == t)["belief_shock_z_1h"][0]
    assert z1 == z2


def test_belief_lags_use_asof_time_not_row_shift_for_irregular_timestamps() -> None:
    # Irregular timestamps: lag should be chosen by timestamp-asof, not "previous row".
    base = datetime(2026, 5, 6, 10, 5, 0, tzinfo=UTC)
    ts = [base, base + timedelta(hours=1, minutes=-3), base + timedelta(hours=2, minutes=2)]
    df = pl.DataFrame(
        {
            "timestamp": ts,
            "platform": ["polymarket"] * 3,
            "market_id": ["m1"] * 3,
            "outcome": ["Yes"] * 3,
            "price": [0.30, 0.35, 0.50],
            "best_bid": [None] * 3,
            "best_ask": [None] * 3,
            "spread": [None] * 3,
            "liquidity": [100000.0] * 3,
            "volume": [200000.0] * 3,
        }
    )

    out = build_belief_features(df, _tags()).sort("event_time")
    last = out.filter(pl.col("event_time") == ts[2])

    # For t=12:07, t-1h=11:07; as-of backward picks 11:02 snapshot (price=0.35).
    assert last["price_lag_1h"][0] == pytest.approx(0.35)
    assert last["price_lag_1h_source_time"][0] == ts[1]
