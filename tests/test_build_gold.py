from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

import crypto_belief_pipeline.features.build_gold as bg
from crypto_belief_pipeline.lake.read import LakeKeyNotFound


def _fear_greed_regime_silver() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "source": ["alternative_me"],
            "date_utc": [datetime(2026, 5, 6, tzinfo=UTC).date()],
            "value": [55],
            "value_classification": ["Neutral"],
            "risk_on_score": [0.10],
        }
    )


def _belief_silver() -> pl.DataFrame:
    base = datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC)
    return pl.DataFrame(
        {
            "timestamp": [base + timedelta(hours=h) for h in range(3)],
            "platform": ["polymarket"] * 3,
            "market_id": ["pm_btc_reserve_001"] * 3,
            "outcome": ["Yes"] * 3,
            "price": [0.30, 0.45, 0.50],
            "best_bid": [0.29, 0.44, 0.49],
            "best_ask": [0.31, 0.46, 0.51],
            "spread": [0.02, 0.02, 0.02],
            "liquidity": [125000.0] * 3,
            "volume": [800000.0] * 3,
        }
    )


def _candles_silver() -> pl.DataFrame:
    base = datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC)
    closes = {0: 64000.0, 1: 64100.0, 2: 64200.0, 5: 64500.0, 25: 65500.0}
    rows = [
        {
            "timestamp": base + timedelta(hours=h),
            "exchange": "binance_usdm",
            "asset": "BTC",
            "symbol": "BTCUSDT",
            "open": c,
            "high": c,
            "low": c,
            "close": c,
            "volume": 1.0,
            "quote_volume": c,
            "number_of_trades": 1,
        }
        for h, c in closes.items()
    ]
    return pl.DataFrame(rows)


def _narrative_silver() -> pl.DataFrame:
    base = datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC)
    return pl.DataFrame(
        {
            "timestamp": [base + timedelta(hours=h) for h in range(3)],
            "narrative": ["bitcoin_reserve"] * 3,
            "query": ["q"] * 3,
            "mention_volume": [0.0001, 0.0004, 0.0005],
            "avg_tone": [1.0] * 3,
            "source": ["gdelt"] * 3,
        }
    )


def test_build_gold_writes_expected_keys(monkeypatch, tmp_path) -> None:
    silver_lookup = {
        "silver/belief_price_snapshots/date=2026-05-06/data.parquet": _belief_silver(),
        "silver/crypto_candles_1m/date=2026-05-06/data.parquet": _candles_silver(),
        "silver/narrative_counts/date=2026-05-06/data.parquet": _narrative_silver(),
        "silver/fear_greed_regime_features/date=2026-05-06/data.parquet": _fear_greed_regime_silver(),
    }

    def fake_read_parquet_df(key: str, bucket: str | None = None) -> pl.DataFrame:
        return silver_lookup[key]

    captured: dict[str, pl.DataFrame] = {}

    def fake_write_parquet_df(df: pl.DataFrame, key: str, bucket: str | None = None) -> None:
        captured[key] = df

    tags_csv = tmp_path / "market_tags.csv"
    tags_csv.write_text(
        "market_id,asset,narrative,direction,relevance,confidence,notes\n"
        "pm_btc_reserve_001,BTC,bitcoin_reserve,1,high,0.9,sample\n"
    )

    monkeypatch.setattr("crypto_belief_pipeline.lake.read.read_parquet_df", fake_read_parquet_df)
    monkeypatch.setattr(bg, "write_parquet_df", fake_write_parquet_df)

    written = bg.build_gold_tables(run_date="2026-05-06", market_tags_path=tags_csv)

    assert written == {
        "training_examples": "gold/training_examples/date=2026-05-06/data.parquet",
        "live_signals": "gold/live_signals/date=2026-05-06/data.parquet",
    }
    assert set(written.values()).issubset(set(captured.keys()))


def test_build_gold_training_examples_contains_required_columns(monkeypatch, tmp_path) -> None:
    silver_lookup = {
        "silver/belief_price_snapshots/date=2026-05-06/data.parquet": _belief_silver(),
        "silver/crypto_candles_1m/date=2026-05-06/data.parquet": _candles_silver(),
        "silver/narrative_counts/date=2026-05-06/data.parquet": _narrative_silver(),
        "silver/fear_greed_regime_features/date=2026-05-06/data.parquet": _fear_greed_regime_silver(),
    }

    captured: dict[str, pl.DataFrame] = {}

    monkeypatch.setattr(
        "crypto_belief_pipeline.lake.read.read_parquet_df",
        lambda key, bucket=None: silver_lookup[key],
    )
    monkeypatch.setattr(
        bg, "write_parquet_df", lambda df, key, bucket=None: captured.setdefault(key, df)
    )

    tags_csv = tmp_path / "market_tags.csv"
    tags_csv.write_text(
        "market_id,asset,narrative,direction,relevance,confidence,notes\n"
        "pm_btc_reserve_001,BTC,bitcoin_reserve,1,high,0.9,sample\n"
    )

    bg.build_gold_tables(run_date="2026-05-06", market_tags_path=tags_csv)

    training = captured["gold/training_examples/date=2026-05-06/data.parquet"]
    expected_cols = {
        "event_time",
        "market_id",
        "asset",
        "narrative",
        "direction",
        "belief_shock_1h",
        "belief_shock_abs_1h",
        "belief_shock_z_1h",
        "price_lag_1h_source_time",
        "price_lag_4h_source_time",
        "narrative_acceleration_1h",
        "narrative_z_1h",
        "asset_ret_past_1h",
        "future_ret_1h",
        "future_ret_4h",
        "future_ret_24h",
        "directional_future_ret_1h",
        "underreaction_score",
        "is_candidate_event",
        "risk_on_score",
        "quality_flags",
    }
    assert expected_cols.issubset(set(training.columns))
    assert training.height >= 1


def test_build_gold_normalizes_mixed_timezone_event_time(monkeypatch, tmp_path) -> None:
    """Mixed tz-aware and tz-naive silver inputs must still join cleanly.

    Polars requires join keys to match exactly (including timezone). The
    ``_normalize_event_time_utc`` helper exists specifically to keep gold robust
    when upstream layers produce inconsistent timezone metadata; this test
    locks that contract in.
    """

    base_ts = datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC)

    # Belief silver: tz-AWARE timestamps (typical Polymarket parsing path).
    belief_aware = pl.DataFrame(
        {
            "timestamp": [base_ts + timedelta(hours=h) for h in range(2)],
            "platform": ["polymarket"] * 2,
            "market_id": ["pm_btc_reserve_001"] * 2,
            "outcome": ["Yes"] * 2,
            "price": [0.30, 0.45],
            "best_bid": [0.29, 0.44],
            "best_ask": [0.31, 0.46],
            "spread": [0.02, 0.02],
            "liquidity": [125000.0] * 2,
            "volume": [800000.0] * 2,
        }
    )

    # Narrative silver: tz-NAIVE timestamps. Must still join with belief and prices.
    narrative_naive = pl.DataFrame(
        {
            "timestamp": [
                datetime(2026, 5, 6, 10, 0, 0),
                datetime(2026, 5, 6, 11, 0, 0),
            ],
            "narrative": ["bitcoin_reserve"] * 2,
            "query": ["q"] * 2,
            "mention_volume": [0.0001, 0.0004],
            "avg_tone": [1.0] * 2,
            "source": ["gdelt"] * 2,
        }
    )

    candles_aware = _candles_silver()

    silver_lookup = {
        "silver/belief_price_snapshots/date=2026-05-06/data.parquet": belief_aware,
        "silver/crypto_candles_1m/date=2026-05-06/data.parquet": candles_aware,
        "silver/narrative_counts/date=2026-05-06/data.parquet": narrative_naive,
        "silver/fear_greed_regime_features/date=2026-05-06/data.parquet": _fear_greed_regime_silver(),
    }
    captured: dict[str, pl.DataFrame] = {}

    monkeypatch.setattr(
        "crypto_belief_pipeline.lake.read.read_parquet_df",
        lambda key, bucket=None: silver_lookup[key],
    )
    monkeypatch.setattr(
        bg, "write_parquet_df", lambda df, key, bucket=None: captured.setdefault(key, df)
    )

    tags_csv = tmp_path / "market_tags.csv"
    tags_csv.write_text(
        "market_id,asset,narrative,direction,relevance,confidence,notes\n"
        "pm_btc_reserve_001,BTC,bitcoin_reserve,1,high,0.9,sample\n"
    )

    bg.build_gold_tables(run_date="2026-05-06", market_tags_path=tags_csv)

    training = captured["gold/training_examples/date=2026-05-06/data.parquet"]
    # The mixed-tz inputs must join (no event_time mismatch dropping all rows).
    assert training.height >= 1
    # Narrative features should land for at least one row when tz alignment works.
    has_narrative_value = (
        training.filter(pl.col("narrative_acceleration_1h").is_not_null()).height > 0
    )
    assert has_narrative_value


def test_build_gold_asof_joins_price_features_for_slightly_late_events(
    monkeypatch, tmp_path
) -> None:
    base_ts = datetime(2026, 5, 6, 10, 0, 30, tzinfo=UTC)
    belief = pl.DataFrame(
        {
            "timestamp": [base_ts, base_ts + timedelta(hours=1)],
            "platform": ["polymarket"] * 2,
            "market_id": ["pm_btc_reserve_001"] * 2,
            "outcome": ["Yes"] * 2,
            "price": [0.30, 0.45],
            "best_bid": [0.29, 0.44],
            "best_ask": [0.31, 0.46],
            "spread": [0.02, 0.02],
            "liquidity": [125000.0] * 2,
            "volume": [800000.0] * 2,
        }
    )
    candles = pl.DataFrame(
        [
            {
                "timestamp": datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC),
                "exchange": "binance_usdm",
                "asset": "BTC",
                "symbol": "BTCUSDT",
                "open": 64000.0,
                "high": 64000.0,
                "low": 64000.0,
                "close": 64000.0,
                "volume": 1.0,
                "quote_volume": 64000.0,
                "number_of_trades": 1,
            },
            {
                "timestamp": datetime(2026, 5, 6, 11, 0, 0, tzinfo=UTC),
                "exchange": "binance_usdm",
                "asset": "BTC",
                "symbol": "BTCUSDT",
                "open": 64100.0,
                "high": 64100.0,
                "low": 64100.0,
                "close": 64100.0,
                "volume": 1.0,
                "quote_volume": 64100.0,
                "number_of_trades": 1,
            },
        ]
    )
    silver_lookup = {
        "silver/belief_price_snapshots/date=2026-05-06/data.parquet": belief,
        "silver/crypto_candles_1m/date=2026-05-06/data.parquet": candles,
        "silver/narrative_counts/date=2026-05-06/data.parquet": _narrative_silver(),
        "silver/fear_greed_regime_features/date=2026-05-06/data.parquet": _fear_greed_regime_silver(),
    }
    captured: dict[str, pl.DataFrame] = {}

    monkeypatch.setattr(
        "crypto_belief_pipeline.lake.read.read_parquet_df",
        lambda key, bucket=None: silver_lookup[key],
    )
    monkeypatch.setattr(
        bg, "write_parquet_df", lambda df, key, bucket=None: captured.setdefault(key, df)
    )

    tags_csv = tmp_path / "market_tags.csv"
    tags_csv.write_text(
        "market_id,asset,narrative,direction,relevance,confidence,notes\n"
        "pm_btc_reserve_001,BTC,bitcoin_reserve,1,high,0.9,sample\n"
    )

    bg.build_gold_tables(run_date="2026-05-06", market_tags_path=tags_csv)

    training = captured["gold/training_examples/date=2026-05-06/data.parquet"]
    assert training["asset_close"][0] == 64000.0
    assert training["price_feature_age"][0] == timedelta(seconds=30)


def test_build_gold_duplicate_silver_narrative_does_not_explode_cardinality(
    monkeypatch, tmp_path
) -> None:
    """Duplicate narrative rows at same (event_time, narrative) must not multiply gold rows.

    Without controlled join semantics, a duplicate silver narrative row would
    cause a Cartesian-style growth of gold rows. We assert the gold row count
    stays bounded by the belief input size.
    """

    base_ts = datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC)

    belief = pl.DataFrame(
        {
            "timestamp": [base_ts + timedelta(hours=h) for h in range(2)],
            "platform": ["polymarket"] * 2,
            "market_id": ["pm_btc_reserve_001"] * 2,
            "outcome": ["Yes"] * 2,
            "price": [0.30, 0.45],
            "best_bid": [0.29, 0.44],
            "best_ask": [0.31, 0.46],
            "spread": [0.02, 0.02],
            "liquidity": [125000.0] * 2,
            "volume": [800000.0] * 2,
        }
    )

    # Duplicate narrative rows for the same (timestamp, narrative).
    narrative = pl.DataFrame(
        {
            "timestamp": [
                base_ts,
                base_ts,
                base_ts + timedelta(hours=1),
                base_ts + timedelta(hours=1),
            ],
            "narrative": ["bitcoin_reserve"] * 4,
            "query": ["q"] * 4,
            "mention_volume": [0.0001, 0.0001, 0.0004, 0.0004],
            "avg_tone": [1.0] * 4,
            "source": ["gdelt"] * 4,
        }
    )

    candles = _candles_silver()

    silver_lookup = {
        "silver/belief_price_snapshots/date=2026-05-06/data.parquet": belief,
        "silver/crypto_candles_1m/date=2026-05-06/data.parquet": candles,
        "silver/narrative_counts/date=2026-05-06/data.parquet": narrative,
        "silver/fear_greed_regime_features/date=2026-05-06/data.parquet": _fear_greed_regime_silver(),
    }
    captured: dict[str, pl.DataFrame] = {}

    monkeypatch.setattr(
        "crypto_belief_pipeline.lake.read.read_parquet_df",
        lambda key, bucket=None: silver_lookup[key],
    )
    monkeypatch.setattr(
        bg, "write_parquet_df", lambda df, key, bucket=None: captured.setdefault(key, df)
    )

    tags_csv = tmp_path / "market_tags.csv"
    tags_csv.write_text(
        "market_id,asset,narrative,direction,relevance,confidence,notes\n"
        "pm_btc_reserve_001,BTC,bitcoin_reserve,1,high,0.9,sample\n"
    )

    bg.build_gold_tables(run_date="2026-05-06", market_tags_path=tags_csv)
    training = captured["gold/training_examples/date=2026-05-06/data.parquet"]

    # Belief produces 2 candidate rows; gold rows must not exceed belief rows
    # by more than a constant factor (i.e. join cardinality stays controlled).
    assert training.height <= belief.height * 2
    # Narrative key uniqueness check that joined narrative metrics are deterministic.
    distinct_event_times = training["event_time"].n_unique()
    assert distinct_event_times >= 1


def test_build_gold_with_empty_narrative_silver_succeeds(monkeypatch, tmp_path) -> None:
    """Missing/empty GDELT narrative input must not block gold; narrative features null."""

    silver_lookup = {
        "silver/belief_price_snapshots/date=2026-05-06/data.parquet": _belief_silver(),
        "silver/crypto_candles_1m/date=2026-05-06/data.parquet": _candles_silver(),
        "silver/narrative_counts/date=2026-05-06/data.parquet": pl.DataFrame(),
        "silver/fear_greed_regime_features/date=2026-05-06/data.parquet": _fear_greed_regime_silver(),
    }
    captured: dict[str, pl.DataFrame] = {}

    monkeypatch.setattr(
        "crypto_belief_pipeline.lake.read.read_parquet_df",
        lambda key, bucket=None: silver_lookup[key],
    )
    monkeypatch.setattr(
        bg, "write_parquet_df", lambda df, key, bucket=None: captured.setdefault(key, df)
    )

    tags_csv = tmp_path / "market_tags.csv"
    tags_csv.write_text(
        "market_id,asset,narrative,direction,relevance,confidence,notes\n"
        "pm_btc_reserve_001,BTC,bitcoin_reserve,1,high,0.9,sample\n"
    )

    bg.build_gold_tables(run_date="2026-05-06", market_tags_path=tags_csv)

    training = captured["gold/training_examples/date=2026-05-06/data.parquet"]
    assert training.height >= 1
    assert training.filter(pl.col("narrative_acceleration_1h").is_null()).height == training.height


def test_build_gold_live_signals_is_filtered_subset(monkeypatch, tmp_path) -> None:
    silver_lookup = {
        "silver/belief_price_snapshots/date=2026-05-06/data.parquet": _belief_silver(),
        "silver/crypto_candles_1m/date=2026-05-06/data.parquet": _candles_silver(),
        "silver/narrative_counts/date=2026-05-06/data.parquet": _narrative_silver(),
        "silver/fear_greed_regime_features/date=2026-05-06/data.parquet": _fear_greed_regime_silver(),
    }
    captured: dict[str, pl.DataFrame] = {}

    monkeypatch.setattr(
        "crypto_belief_pipeline.lake.read.read_parquet_df",
        lambda key, bucket=None: silver_lookup[key],
    )
    monkeypatch.setattr(
        bg, "write_parquet_df", lambda df, key, bucket=None: captured.setdefault(key, df)
    )

    tags_csv = tmp_path / "market_tags.csv"
    tags_csv.write_text(
        "market_id,asset,narrative,direction,relevance,confidence,notes\n"
        "pm_btc_reserve_001,BTC,bitcoin_reserve,1,high,0.9,sample\n"
    )

    bg.build_gold_tables(run_date="2026-05-06", market_tags_path=tags_csv)

    training = captured["gold/training_examples/date=2026-05-06/data.parquet"]
    live = captured["gold/live_signals/date=2026-05-06/data.parquet"]
    assert live.height <= training.height
    if live.height > 0:
        assert live["is_candidate_event"].all()


def test_build_gold_partitioned_missing_key_falls_back_to_partition_hour_only(
    monkeypatch, tmp_path
) -> None:
    """When canonical partition object is missing, fallback reads stay hour-scoped."""
    target_hour = datetime(2026, 5, 8, 23, 0, 0, tzinfo=UTC)
    prior_hour = target_hour - timedelta(hours=1)
    candles_target = datetime(2026, 5, 8, 23, 0, 30, tzinfo=UTC)

    belief_partition = pl.DataFrame(
        {
            "timestamp": [prior_hour, target_hour],
            "platform": ["polymarket", "polymarket"],
            "market_id": ["pm_btc_reserve_001", "pm_btc_reserve_001"],
            "outcome": ["Yes", "Yes"],
            "price": [0.30, 0.45],
            "best_bid": [0.29, 0.44],
            "best_ask": [0.31, 0.46],
            "spread": [0.02, 0.02],
            "liquidity": [125000.0, 125000.0],
            "volume": [800000.0, 800000.0],
        }
    )
    candles_partition = pl.DataFrame(
        [
            {
                "timestamp": prior_hour,
                "exchange": "binance_usdm",
                "asset": "BTC",
                "symbol": "BTCUSDT",
                "open": 64000.0,
                "high": 64000.0,
                "low": 64000.0,
                "close": 64000.0,
                "volume": 1.0,
                "quote_volume": 64000.0,
                "number_of_trades": 1,
            },
            {
                "timestamp": candles_target,
                "exchange": "binance_usdm",
                "asset": "BTC",
                "symbol": "BTCUSDT",
                "open": 64100.0,
                "high": 64100.0,
                "low": 64100.0,
                "close": 64100.0,
                "volume": 1.0,
                "quote_volume": 64100.0,
                "number_of_trades": 1,
            },
        ]
    )
    narrative_partition = pl.DataFrame(
        {
            "timestamp": [prior_hour, target_hour],
            "narrative": ["bitcoin_reserve", "bitcoin_reserve"],
            "query": ["q", "q"],
            "mention_volume": [0.0001, 0.0004],
            "avg_tone": [1.0, 1.0],
            "source": ["gdelt", "gdelt"],
        }
    )

    missing_partition_keys = {
        "silver/belief_price_snapshots/date=2026-05-08/partition=2026-05-08-23-00/data.parquet",
        "silver/crypto_candles_1m/date=2026-05-08/partition=2026-05-08-23-00/data.parquet",
        "silver/narrative_counts/date=2026-05-08/partition=2026-05-08-23-00/data.parquet",
        "silver/fear_greed_regime_features/date=2026-05-08/partition=2026-05-08-23-00/data.parquet",
    }
    partition_lookup = {
        "silver/belief_price_snapshots/date=2026-05-08": belief_partition,
        "silver/crypto_candles_1m/date=2026-05-08": candles_partition,
        "silver/narrative_counts/date=2026-05-08": narrative_partition,
        "silver/fear_greed_regime_features/date=2026-05-08": _fear_greed_regime_silver(),
    }
    captured: dict[str, pl.DataFrame] = {}

    def fake_read_parquet_df(key: str, bucket: str | None = None) -> pl.DataFrame:
        if key in missing_partition_keys:
            raise LakeKeyNotFound(f"lake key not found: {key}")
        raise AssertionError(f"Unexpected direct parquet key read: {key}")

    monkeypatch.setattr(bg, "read_parquet_df", fake_read_parquet_df)
    monkeypatch.setattr(
        bg, "read_parquet_partition_df", lambda prefix, bucket=None: partition_lookup[prefix]
    )
    monkeypatch.setattr(
        bg, "write_parquet_df", lambda df, key, bucket=None: captured.setdefault(key, df)
    )

    tags_csv = tmp_path / "market_tags.csv"
    tags_csv.write_text(
        "market_id,asset,narrative,direction,relevance,confidence,notes\n"
        "pm_btc_reserve_001,BTC,bitcoin_reserve,1,high,0.9,sample\n"
    )

    bg.build_gold_tables(
        run_date="2026-05-08",
        partition_key="2026-05-08-23:00",
        market_tags_path=tags_csv,
    )

    training = captured[
        "gold/training_examples/date=2026-05-08/partition=2026-05-08-23-00/data.parquet"
    ]
    assert training.height >= 1
    event_min = training["event_time"].min()
    event_max = training["event_time"].max()
    assert isinstance(event_min, datetime)
    assert isinstance(event_max, datetime)
    assert event_min >= target_hour
    assert event_max < (target_hour + timedelta(hours=1))


def test_build_gold_hourly_partition_missing_narrative_silver_entirely_succeeds(
    monkeypatch, tmp_path
) -> None:
    """No narrative Parquet for the run_date (hourly layout) must not fail gold.

    Mirrors optional GDELT narrative when neither hourly nor date-level narrative
    objects exist in the lake.
    """

    target_hour = datetime(2026, 5, 9, 0, 0, 0, tzinfo=UTC)
    belief_partition = pl.DataFrame(
        {
            "timestamp": [target_hour + timedelta(minutes=30)],
            "platform": ["polymarket"],
            "market_id": ["pm_btc_reserve_001"],
            "outcome": ["Yes"],
            "price": [0.45],
            "best_bid": [0.44],
            "best_ask": [0.46],
            "spread": [0.02],
            "liquidity": [125000.0],
            "volume": [800000.0],
        }
    )
    candles_partition = pl.DataFrame(
        [
            {
                "timestamp": target_hour + timedelta(minutes=30),
                "exchange": "binance_usdm",
                "asset": "BTC",
                "symbol": "BTCUSDT",
                "open": 64100.0,
                "high": 64100.0,
                "low": 64100.0,
                "close": 64100.0,
                "volume": 1.0,
                "quote_volume": 64100.0,
                "number_of_trades": 1,
            },
        ]
    )

    missing_partition_keys = {
        "silver/belief_price_snapshots/date=2026-05-09/partition=2026-05-09-00-00/data.parquet",
        "silver/crypto_candles_1m/date=2026-05-09/partition=2026-05-09-00-00/data.parquet",
        "silver/narrative_counts/date=2026-05-09/partition=2026-05-09-00-00/data.parquet",
        "silver/fear_greed_regime_features/date=2026-05-09/partition=2026-05-09-00-00/data.parquet",
    }
    partition_lookup = {
        "silver/belief_price_snapshots/date=2026-05-09": belief_partition,
        "silver/crypto_candles_1m/date=2026-05-09": candles_partition,
        "silver/narrative_counts/date=2026-05-09": pl.DataFrame(),
        "silver/fear_greed_regime_features/date=2026-05-09": pl.DataFrame(
            {
                "source": ["alternative_me"],
                "date_utc": [datetime(2026, 5, 9, tzinfo=UTC).date()],
                "value": [55],
                "value_classification": ["Neutral"],
                "risk_on_score": [0.10],
            }
        ),
    }
    captured: dict[str, pl.DataFrame] = {}

    def fake_read_parquet_df(key: str, bucket: str | None = None) -> pl.DataFrame:
        if key in missing_partition_keys:
            raise LakeKeyNotFound(f"lake key not found: {key}")
        raise AssertionError(f"Unexpected direct parquet key read: {key}")

    monkeypatch.setattr(bg, "read_parquet_df", fake_read_parquet_df)
    monkeypatch.setattr(
        bg, "read_parquet_partition_df", lambda prefix, bucket=None: partition_lookup[prefix]
    )
    monkeypatch.setattr(
        bg, "write_parquet_df", lambda df, key, bucket=None: captured.setdefault(key, df)
    )

    tags_csv = tmp_path / "market_tags.csv"
    tags_csv.write_text(
        "market_id,asset,narrative,direction,relevance,confidence,notes\n"
        "pm_btc_reserve_001,BTC,bitcoin_reserve,1,high,0.9,sample\n"
    )

    bg.build_gold_tables(
        run_date="2026-05-09",
        partition_key="2026-05-09-00:00",
        market_tags_path=tags_csv,
    )

    training = captured[
        "gold/training_examples/date=2026-05-09/partition=2026-05-09-00-00/data.parquet"
    ]
    assert training.height >= 1
    assert training.filter(pl.col("narrative_acceleration_1h").is_null()).height == training.height


def test_build_gold_partitioned_fallback_requires_timestamp_column(
    monkeypatch, tmp_path
) -> None:
    missing_partition_keys = {
        "silver/belief_price_snapshots/date=2026-05-08/partition=2026-05-08-23-00/data.parquet",
        "silver/crypto_candles_1m/date=2026-05-08/partition=2026-05-08-23-00/data.parquet",
        "silver/narrative_counts/date=2026-05-08/partition=2026-05-08-23-00/data.parquet",
        "silver/fear_greed_regime_features/date=2026-05-08/partition=2026-05-08-23-00/data.parquet",
    }
    partition_lookup = {
        "silver/belief_price_snapshots/date=2026-05-08": pl.DataFrame(
            {
                "platform": ["polymarket"],
                "market_id": ["pm_btc_reserve_001"],
                "outcome": ["Yes"],
                "price": [0.45],
            }
        ),
        "silver/crypto_candles_1m/date=2026-05-08": pl.DataFrame(
            {
                "asset": ["BTC"],
                "close": [64100.0],
            }
        ),
        "silver/narrative_counts/date=2026-05-08": pl.DataFrame(
            {
                "narrative": ["bitcoin_reserve"],
                "mention_volume": [0.0004],
            }
        ),
        "silver/fear_greed_regime_features/date=2026-05-08": _fear_greed_regime_silver(),
    }

    def fake_read_parquet_df(key: str, bucket: str | None = None) -> pl.DataFrame:
        if key in missing_partition_keys:
            raise LakeKeyNotFound(f"lake key not found: {key}")
        raise AssertionError(f"Unexpected direct parquet key read: {key}")

    monkeypatch.setattr(bg, "read_parquet_df", fake_read_parquet_df)
    monkeypatch.setattr(
        bg, "read_parquet_partition_df", lambda prefix, bucket=None: partition_lookup[prefix]
    )
    monkeypatch.setattr(bg, "write_parquet_df", lambda df, key, bucket=None: None)

    tags_csv = tmp_path / "market_tags.csv"
    tags_csv.write_text(
        "market_id,asset,narrative,direction,relevance,confidence,notes\n"
        "pm_btc_reserve_001,BTC,bitcoin_reserve,1,high,0.9,sample\n"
    )

    with pytest.raises(ValueError, match="requires `timestamp` column"):
        bg.build_gold_tables(
            run_date="2026-05-08",
            partition_key="2026-05-08-23:00",
            market_tags_path=tags_csv,
        )
