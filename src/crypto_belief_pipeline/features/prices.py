from __future__ import annotations

import polars as pl


def build_price_features(candles: pl.DataFrame) -> pl.DataFrame:
    """Build past- and forward-return features per asset using exact timestamp joins.

    For each candle row at time ``t``, we compute close prices at ``t-1h``,
    ``t-4h``, ``t+1h``, ``t+4h`` and ``t+24h`` via self-joins on
    ``(asset, timestamp)``. If a target timestamp does not exist in the input,
    the corresponding column is null. This is sufficient for the MVP because
    Binance 1m candles plus the extended sample data align on whole hours; a
    later step can add as-of joins for irregular event times.
    """

    if candles.is_empty():
        return _empty_output()

    base = candles.sort(["asset", "timestamp"]).select(
        pl.col("timestamp").alias("event_time"),
        pl.col("asset"),
        pl.col("close").alias("asset_close"),
    )

    base = base.with_columns(
        (pl.col("event_time") - pl.duration(hours=1)).alias("event_time_minus_1h"),
        (pl.col("event_time") - pl.duration(hours=4)).alias("event_time_minus_4h"),
        (pl.col("event_time") + pl.duration(hours=1)).alias("event_time_plus_1h"),
        (pl.col("event_time") + pl.duration(hours=4)).alias("event_time_plus_4h"),
        (pl.col("event_time") + pl.duration(hours=24)).alias("event_time_plus_24h"),
    )

    lookup = candles.select(
        pl.col("timestamp"),
        pl.col("asset"),
        pl.col("close"),
    )

    joined = (
        base.join(
            lookup.rename({"timestamp": "event_time_minus_1h", "close": "asset_close_lag_1h"}),
            on=["asset", "event_time_minus_1h"],
            how="left",
        )
        .join(
            lookup.rename({"timestamp": "event_time_minus_4h", "close": "asset_close_lag_4h"}),
            on=["asset", "event_time_minus_4h"],
            how="left",
        )
        .join(
            lookup.rename({"timestamp": "event_time_plus_1h", "close": "future_close_1h"}),
            on=["asset", "event_time_plus_1h"],
            how="left",
        )
        .join(
            lookup.rename({"timestamp": "event_time_plus_4h", "close": "future_close_4h"}),
            on=["asset", "event_time_plus_4h"],
            how="left",
        )
        .join(
            lookup.rename({"timestamp": "event_time_plus_24h", "close": "future_close_24h"}),
            on=["asset", "event_time_plus_24h"],
            how="left",
        )
    )

    with_returns = joined.with_columns(
        (pl.col("asset_close") / pl.col("asset_close_lag_1h") - 1.0).alias("asset_ret_past_1h"),
        (pl.col("asset_close") / pl.col("asset_close_lag_4h") - 1.0).alias("asset_ret_past_4h"),
        (pl.col("future_close_1h") / pl.col("asset_close") - 1.0).alias("future_ret_1h"),
        (pl.col("future_close_4h") / pl.col("asset_close") - 1.0).alias("future_ret_4h"),
        (pl.col("future_close_24h") / pl.col("asset_close") - 1.0).alias("future_ret_24h"),
    )

    return with_returns.select(
        "event_time",
        "asset",
        "asset_close",
        "asset_close_lag_1h",
        "asset_close_lag_4h",
        "asset_ret_past_1h",
        "asset_ret_past_4h",
        "future_close_1h",
        "future_close_4h",
        "future_close_24h",
        "future_ret_1h",
        "future_ret_4h",
        "future_ret_24h",
    )


def _empty_output() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "event_time": pl.Datetime,
            "asset": pl.String,
            "asset_close": pl.Float64,
            "asset_close_lag_1h": pl.Float64,
            "asset_close_lag_4h": pl.Float64,
            "asset_ret_past_1h": pl.Float64,
            "asset_ret_past_4h": pl.Float64,
            "future_close_1h": pl.Float64,
            "future_close_4h": pl.Float64,
            "future_close_24h": pl.Float64,
            "future_ret_1h": pl.Float64,
            "future_ret_4h": pl.Float64,
            "future_ret_24h": pl.Float64,
        }
    )


__all__ = ["build_price_features"]
