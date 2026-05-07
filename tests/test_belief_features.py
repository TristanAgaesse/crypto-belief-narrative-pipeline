from __future__ import annotations

from datetime import UTC, datetime

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
