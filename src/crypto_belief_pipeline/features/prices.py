from __future__ import annotations

from datetime import timedelta
from typing import Literal

import polars as pl

_PRICE_ASOF_TOLERANCE = timedelta(minutes=2)


def build_price_features(candles: pl.DataFrame) -> pl.DataFrame:
    """Build past- and forward-return features per asset using bounded as-of joins.

    For each candle row at time ``t``, we compute close prices at ``t-1h``,
    ``t-4h``, ``t+1h``, ``t+4h`` and ``t+24h``. Past features use the latest
    candle at or before the target lag; labels use the earliest candle at or
    after the target horizon. Both directions are bounded by a small tolerance
    so sparse data does not silently attach stale prices.
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

    joined = _join_close_asof(
        base,
        lookup,
        target_col="event_time_minus_1h",
        close_col="asset_close_lag_1h",
        source_time_col="asset_close_lag_1h_source_time",
        strategy="backward",
    )
    joined = _join_close_asof(
        joined,
        lookup,
        target_col="event_time_minus_4h",
        close_col="asset_close_lag_4h",
        source_time_col="asset_close_lag_4h_source_time",
        strategy="backward",
    )
    joined = _join_close_asof(
        joined,
        lookup,
        target_col="event_time_plus_1h",
        close_col="future_close_1h",
        source_time_col="future_close_1h_source_time",
        strategy="forward",
    )
    joined = _join_close_asof(
        joined,
        lookup,
        target_col="event_time_plus_4h",
        close_col="future_close_4h",
        source_time_col="future_close_4h_source_time",
        strategy="forward",
    )
    joined = _join_close_asof(
        joined,
        lookup,
        target_col="event_time_plus_24h",
        close_col="future_close_24h",
        source_time_col="future_close_24h_source_time",
        strategy="forward",
    )

    with_audit = joined.with_columns(
        (pl.col("event_time") - pl.col("asset_close_lag_1h_source_time")).alias(
            "asset_close_lag_1h_age"
        ),
        (pl.col("event_time") - pl.col("asset_close_lag_4h_source_time")).alias(
            "asset_close_lag_4h_age"
        ),
        (pl.col("future_close_1h_source_time") - pl.col("event_time")).alias("future_close_1h_age"),
        (pl.col("future_close_4h_source_time") - pl.col("event_time")).alias("future_close_4h_age"),
        (pl.col("future_close_24h_source_time") - pl.col("event_time")).alias(
            "future_close_24h_age"
        ),
    )

    with_returns = with_audit.with_columns(
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
        "asset_close_lag_1h_source_time",
        "asset_close_lag_4h_source_time",
        "asset_close_lag_1h_age",
        "asset_close_lag_4h_age",
        "asset_ret_past_1h",
        "asset_ret_past_4h",
        "future_close_1h",
        "future_close_4h",
        "future_close_24h",
        "future_close_1h_source_time",
        "future_close_4h_source_time",
        "future_close_24h_source_time",
        "future_close_1h_age",
        "future_close_4h_age",
        "future_close_24h_age",
        "future_ret_1h",
        "future_ret_4h",
        "future_ret_24h",
    )


def _join_close_asof(
    left: pl.DataFrame,
    lookup: pl.DataFrame,
    *,
    target_col: str,
    close_col: str,
    source_time_col: str,
    strategy: Literal["backward", "forward"],
) -> pl.DataFrame:
    right = lookup.rename({"timestamp": source_time_col, "close": close_col}).sort(
        ["asset", source_time_col]
    )
    return left.sort(["asset", target_col]).join_asof(
        right,
        left_on=target_col,
        right_on=source_time_col,
        by="asset",
        strategy=strategy,
        tolerance=_PRICE_ASOF_TOLERANCE,
    )


def _empty_output() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "event_time": pl.Datetime,
            "asset": pl.String,
            "asset_close": pl.Float64,
            "asset_close_lag_1h": pl.Float64,
            "asset_close_lag_4h": pl.Float64,
            "asset_close_lag_1h_source_time": pl.Datetime,
            "asset_close_lag_4h_source_time": pl.Datetime,
            "asset_close_lag_1h_age": pl.Duration,
            "asset_close_lag_4h_age": pl.Duration,
            "asset_ret_past_1h": pl.Float64,
            "asset_ret_past_4h": pl.Float64,
            "future_close_1h": pl.Float64,
            "future_close_4h": pl.Float64,
            "future_close_24h": pl.Float64,
            "future_close_1h_source_time": pl.Datetime,
            "future_close_4h_source_time": pl.Datetime,
            "future_close_24h_source_time": pl.Datetime,
            "future_close_1h_age": pl.Duration,
            "future_close_4h_age": pl.Duration,
            "future_close_24h_age": pl.Duration,
            "future_ret_1h": pl.Float64,
            "future_ret_4h": pl.Float64,
            "future_ret_24h": pl.Float64,
        }
    )


__all__ = ["build_price_features"]
