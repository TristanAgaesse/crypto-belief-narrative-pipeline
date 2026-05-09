from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from crypto_belief_pipeline.features.fear_greed import join_fear_greed_asof
from crypto_belief_pipeline.transform.normalize_fear_greed import build_regime_features


def test_build_regime_features_emits_risk_on_score() -> None:
    daily = pl.DataFrame(
        {
            "source": ["alternative_me"] * 5,
            "date_utc": [
                datetime(2026, 5, 1, tzinfo=UTC).date(),
                datetime(2026, 5, 2, tzinfo=UTC).date(),
                datetime(2026, 5, 3, tzinfo=UTC).date(),
                datetime(2026, 5, 4, tzinfo=UTC).date(),
                datetime(2026, 5, 5, tzinfo=UTC).date(),
            ],
            "value": [10, 20, 30, 40, 50],
            "value_classification": ["Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"],
        }
    )
    feats = build_regime_features(daily)
    assert "risk_on_score" in feats.columns
    assert feats.height == 5
    assert feats["risk_on_score"].min() <= feats["risk_on_score"].max()


def test_join_fear_greed_asof_adds_null_risk_on_score_when_missing_event_time() -> None:
    events = pl.DataFrame({"market_id": ["m1"], "asset": ["BTC"]})
    regime = pl.DataFrame(
        {
            "source": ["alternative_me"],
            "date_utc": [datetime(2026, 5, 6, tzinfo=UTC).date()],
            "risk_on_score": [0.2],
            "value": [50],
        }
    )
    out = join_fear_greed_asof(events, fear_greed_regime=regime)
    assert "risk_on_score" in out.columns
    assert out["risk_on_score"][0] is None


def test_join_fear_greed_asof_adds_null_risk_on_score_when_regime_missing_date_utc() -> None:
    events = pl.DataFrame(
        {
            "event_time": [datetime(2026, 5, 6, 10, 0, tzinfo=UTC)],
            "market_id": ["m1"],
        }
    )
    regime = pl.DataFrame({"source": ["alternative_me"], "risk_on_score": [0.1], "value": [40]})
    out = join_fear_greed_asof(events, fear_greed_regime=regime)
    assert "risk_on_score" in out.columns
    assert out["risk_on_score"][0] is None


def test_join_fear_greed_asof_adds_risk_on_score_when_regime_table_omits_it() -> None:
    events = pl.DataFrame(
        {
            "event_time": [datetime(2026, 5, 6, 10, 0, tzinfo=UTC)],
            "market_id": ["m1"],
        }
    )
    regime = pl.DataFrame(
        {
            "source": ["alternative_me"],
            "date_utc": [datetime(2026, 5, 6, tzinfo=UTC).date()],
            "value": [50],
        }
    )
    out = join_fear_greed_asof(events, fear_greed_regime=regime)
    assert "risk_on_score" in out.columns
    assert out["risk_on_score"][0] is None


def test_join_fear_greed_asof_empty_after_source_filter_still_has_risk_on_score() -> None:
    events = pl.DataFrame(
        {
            "event_time": [datetime(2026, 5, 6, 10, 0, tzinfo=UTC)],
            "market_id": ["m1"],
        }
    )
    regime = pl.DataFrame(
        {
            "source": ["other"],
            "date_utc": [datetime(2026, 5, 6, tzinfo=UTC).date()],
            "risk_on_score": [0.9],
            "value": [80],
        }
    )
    out = join_fear_greed_asof(events, fear_greed_regime=regime)
    assert "risk_on_score" in out.columns
    assert out["risk_on_score"][0] is None


def test_join_fear_greed_asof_is_causal() -> None:
    events = pl.DataFrame(
        {
            "event_time": [
                datetime(2026, 5, 6, 10, 30, tzinfo=UTC),
                datetime(2026, 5, 7, 9, 0, tzinfo=UTC),
            ],
            "market_id": ["m1", "m2"],
            "asset": ["BTC", "BTC"],
            "narrative": ["n", "n"],
        }
    )
    regime = pl.DataFrame(
        {
            "source": ["alternative_me", "alternative_me"],
            "date_utc": [datetime(2026, 5, 6, tzinfo=UTC).date(), datetime(2026, 5, 8, tzinfo=UTC).date()],
            "risk_on_score": [0.1, 0.9],
            "value": [55, 90],
        }
    )
    joined = join_fear_greed_asof(events, fear_greed_regime=regime)
    assert joined["risk_on_score"][0] == 0.1
    # Second event is 2026-05-07; it must not see the 2026-05-08 value.
    assert joined["risk_on_score"][1] == 0.1

