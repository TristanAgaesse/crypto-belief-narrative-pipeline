from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from crypto_belief_pipeline.features.narrative import build_narrative_features


def _ts(hour: int) -> datetime:
    return datetime(2026, 5, 6, hour, 0, 0, tzinfo=UTC)


def _counts(volumes: list[float], narrative: str = "bitcoin_reserve") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "timestamp": [_ts(10 + i) for i in range(len(volumes))],
            "narrative": [narrative] * len(volumes),
            "query": ["q"] * len(volumes),
            "mention_volume": volumes,
            "avg_tone": [1.0] * len(volumes),
            "source": ["gdelt"] * len(volumes),
        }
    )


def test_narrative_acceleration_uses_lag() -> None:
    counts = _counts([0.0001, 0.0004])
    out = build_narrative_features(counts).sort("event_time")
    second = out.filter(pl.col("event_time") == _ts(11))
    assert second["narrative_volume_lag_1h"][0] == pytest.approx(0.0001)
    assert second["narrative_acceleration_1h"][0] == pytest.approx(4.0)


def test_narrative_first_row_has_null_lag() -> None:
    counts = _counts([0.0001, 0.0004, 0.0005])
    out = build_narrative_features(counts).sort("event_time")
    first = out.filter(pl.col("event_time") == _ts(10))
    assert first["narrative_volume_lag_1h"][0] is None
    assert first["narrative_acceleration_1h"][0] is None


def test_narrative_trailing_does_not_use_future_rows() -> None:
    """Trailing mean at row n must only use rows < n."""
    counts = _counts([0.001, 0.002, 0.003, 0.004])
    out = build_narrative_features(counts).sort("event_time")
    third = out.filter(pl.col("event_time") == _ts(12))
    expected_mean = (0.001 + 0.002) / 2
    assert third["narrative_volume_trailing_mean_24h"][0] == pytest.approx(expected_mean)


def test_narrative_z_score_floor_avoids_division_by_zero() -> None:
    counts = _counts([0.001, 0.001, 0.001])
    out = build_narrative_features(counts).sort("event_time")
    z_values = [v for v in out["narrative_z_1h"].to_list() if v is not None]
    assert all(abs(v) < 1e6 for v in z_values)
