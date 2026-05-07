from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

import crypto_belief_pipeline.features.build_gold as bg


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

    monkeypatch.setattr(bg, "read_parquet_df", fake_read_parquet_df)
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
    }

    captured: dict[str, pl.DataFrame] = {}

    monkeypatch.setattr(bg, "read_parquet_df", lambda key, bucket=None: silver_lookup[key])
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
        "narrative_acceleration_1h",
        "narrative_z_1h",
        "asset_ret_past_1h",
        "future_ret_1h",
        "future_ret_4h",
        "future_ret_24h",
        "directional_future_ret_1h",
        "underreaction_score",
        "is_candidate_event",
        "quality_flags",
    }
    assert expected_cols.issubset(set(training.columns))
    assert training.height >= 1


def test_build_gold_live_signals_is_filtered_subset(monkeypatch, tmp_path) -> None:
    silver_lookup = {
        "silver/belief_price_snapshots/date=2026-05-06/data.parquet": _belief_silver(),
        "silver/crypto_candles_1m/date=2026-05-06/data.parquet": _candles_silver(),
        "silver/narrative_counts/date=2026-05-06/data.parquet": _narrative_silver(),
    }
    captured: dict[str, pl.DataFrame] = {}

    monkeypatch.setattr(bg, "read_parquet_df", lambda key, bucket=None: silver_lookup[key])
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
