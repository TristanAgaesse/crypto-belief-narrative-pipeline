from __future__ import annotations

import polars as pl

_TRAILING_WINDOW = 24
_FLOOR = 1e-9


def build_narrative_features(narrative_counts: pl.DataFrame) -> pl.DataFrame:
    """Build narrative acceleration features per narrative.

    Trailing mean/std windows use prior rows only (``shift(1).rolling_*``) to
    avoid look-ahead. Falls back gracefully on small frames where the rolling
    window has insufficient history.
    """

    if narrative_counts.is_empty():
        return _empty_output()

    sorted_df = narrative_counts.sort(["narrative", "timestamp"])

    with_basics = sorted_df.with_columns(
        pl.col("mention_volume").alias("narrative_volume"),
        pl.col("mention_volume").shift(1).over("narrative").alias("narrative_volume_lag_1h"),
    )

    with_rolling = with_basics.with_columns(
        pl.col("narrative_volume")
        .shift(1)
        .rolling_mean(window_size=_TRAILING_WINDOW, min_samples=1)
        .over("narrative")
        .alias("narrative_volume_trailing_mean_24h"),
        pl.col("narrative_volume")
        .shift(1)
        .rolling_std(window_size=_TRAILING_WINDOW, min_samples=1)
        .over("narrative")
        .alias("narrative_volume_trailing_std_24h"),
    )

    with_fallbacks = with_rolling.with_columns(
        pl.col("narrative_volume_trailing_mean_24h")
        .fill_null(pl.col("narrative_volume_lag_1h"))
        .alias("narrative_volume_trailing_mean_24h"),
        pl.col("narrative_volume_trailing_std_24h")
        .fill_null(_FLOOR)
        .alias("narrative_volume_trailing_std_24h"),
    )

    lag_floor = (
        pl.when(pl.col("narrative_volume_lag_1h") < _FLOOR)
        .then(_FLOOR)
        .otherwise(pl.col("narrative_volume_lag_1h"))
    )
    std_floor = (
        pl.when(pl.col("narrative_volume_trailing_std_24h") < _FLOOR)
        .then(_FLOOR)
        .otherwise(pl.col("narrative_volume_trailing_std_24h"))
    )

    with_features = with_fallbacks.with_columns(
        (pl.col("narrative_volume") / lag_floor).alias("narrative_acceleration_1h"),
        (
            (pl.col("narrative_volume") - pl.col("narrative_volume_trailing_mean_24h")) / std_floor
        ).alias("narrative_z_1h"),
    )

    out = with_features.rename({"timestamp": "event_time"}).select(
        "event_time",
        "narrative",
        "narrative_volume",
        "narrative_volume_lag_1h",
        "narrative_volume_trailing_mean_24h",
        "narrative_acceleration_1h",
        "narrative_z_1h",
    )

    return out


def _empty_output() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "event_time": pl.Datetime,
            "narrative": pl.String,
            "narrative_volume": pl.Float64,
            "narrative_volume_lag_1h": pl.Float64,
            "narrative_volume_trailing_mean_24h": pl.Float64,
            "narrative_acceleration_1h": pl.Float64,
            "narrative_z_1h": pl.Float64,
        }
    )
