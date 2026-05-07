from __future__ import annotations

import polars as pl

_BELIEF_SHOCK_STD_FLOOR = 0.01
_Z_STD_WINDOW = 24


def _has_yes_outcome(belief_snapshots: pl.DataFrame) -> bool:
    if "outcome" not in belief_snapshots.columns:
        return False
    if belief_snapshots.is_empty():
        return False
    yes = belief_snapshots.filter(pl.col("outcome") == "Yes")
    return yes.height > 0


def build_belief_features(
    belief_snapshots: pl.DataFrame,
    market_tags: pl.DataFrame,
) -> pl.DataFrame:
    """Build belief-shock features per (market_id, asset, narrative).

    - Filters to ``outcome == "Yes"`` if any such row exists in the input.
    - Inner-joins ``market_tags`` on ``market_id`` (untagged markets are excluded).
    - Computes 1h/4h directional belief shocks plus a 1h z-score.
    """

    if belief_snapshots.is_empty() or market_tags.is_empty():
        return _empty_belief_output()

    snapshots = belief_snapshots
    if _has_yes_outcome(snapshots):
        snapshots = snapshots.filter(pl.col("outcome") == "Yes")

    joined = snapshots.join(market_tags, on="market_id", how="inner")
    if joined.is_empty():
        return _empty_belief_output()

    sorted_df = joined.sort(["market_id", "asset", "narrative", "timestamp"])

    group_cols = ["market_id", "asset", "narrative"]

    # Timestamp-aware as-of lags: for each event_time t, pick the latest snapshot at or before
    # t-1h / t-4h within the same (market_id, asset, narrative) group.
    base = sorted_df.select(
        pl.col("timestamp").alias("event_time"),
        pl.col("market_id"),
        pl.col("asset"),
        pl.col("narrative"),
        pl.col("direction"),
        pl.col("relevance"),
        pl.col("confidence"),
        pl.col("price"),
        pl.col("spread"),
        pl.col("liquidity"),
        pl.col("volume"),
    ).with_columns(
        (pl.col("event_time") - pl.duration(hours=1)).alias("_t_minus_1h"),
        (pl.col("event_time") - pl.duration(hours=4)).alias("_t_minus_4h"),
    )

    lookup = sorted_df.select(
        pl.col("timestamp").alias("_ts"),
        pl.col("market_id"),
        pl.col("asset"),
        pl.col("narrative"),
        pl.col("price").alias("_price"),
    )

    with_lags = (
        base.join_asof(
            lookup,
            left_on="_t_minus_1h",
            right_on="_ts",
            by=group_cols,
            strategy="backward",
        )
        .rename(
            {
                "_price": "price_lag_1h",
                "_ts": "price_lag_1h_source_time",
            }
        )
        .join_asof(
            lookup,
            left_on="_t_minus_4h",
            right_on="_ts",
            by=group_cols,
            strategy="backward",
        )
        .rename(
            {
                "_price": "price_lag_4h",
                "_ts": "price_lag_4h_source_time",
            }
        )
        .drop("_t_minus_1h", "_t_minus_4h")
        .with_columns(
            (pl.col("direction") * (pl.col("price") - pl.col("price_lag_1h"))).alias(
                "belief_shock_1h"
            ),
            (pl.col("direction") * (pl.col("price") - pl.col("price_lag_4h"))).alias(
                "belief_shock_4h"
            ),
            # PIT audit: how stale were the chosen lag snapshots?
            (pl.col("event_time") - pl.col("price_lag_1h_source_time")).alias("price_lag_1h_age"),
            (pl.col("event_time") - pl.col("price_lag_4h_source_time")).alias("price_lag_4h_age"),
        )
    )

    with_abs = with_lags.with_columns(
        pl.col("belief_shock_1h").abs().alias("belief_shock_abs_1h"),
        pl.col("belief_shock_4h").abs().alias("belief_shock_abs_4h"),
    )

    # Causal scaling: the z-score denominator must not use future rows.
    # We compute a trailing window std over *prior* shocks only.
    denom = (
        pl.col("belief_shock_1h")
        .shift(1)
        .rolling_std(window_size=_Z_STD_WINDOW, min_samples=2)
        .over(group_cols)
        .fill_null(_BELIEF_SHOCK_STD_FLOOR)
    )
    floored_denom = (
        pl.when(denom < _BELIEF_SHOCK_STD_FLOOR).then(_BELIEF_SHOCK_STD_FLOOR).otherwise(denom)
    )
    with_z = with_abs.with_columns(
        (pl.col("belief_shock_1h") / floored_denom).alias("belief_shock_z_1h"),
    )

    out = with_z.select(
        "event_time",
        "market_id",
        "asset",
        "narrative",
        "direction",
        "relevance",
        "confidence",
        "price",
        "price_lag_1h",
        "price_lag_4h",
        "price_lag_1h_source_time",
        "price_lag_4h_source_time",
        "price_lag_1h_age",
        "price_lag_4h_age",
        "belief_shock_1h",
        "belief_shock_4h",
        "belief_shock_abs_1h",
        "belief_shock_abs_4h",
        "belief_shock_z_1h",
        pl.col("spread"),
        pl.col("liquidity"),
        pl.col("volume"),
    )

    return out


def _empty_belief_output() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "event_time": pl.Datetime,
            "market_id": pl.String,
            "asset": pl.String,
            "narrative": pl.String,
            "direction": pl.Int64,
            "relevance": pl.String,
            "confidence": pl.Float64,
            "price": pl.Float64,
            "price_lag_1h": pl.Float64,
            "price_lag_4h": pl.Float64,
            "price_lag_1h_source_time": pl.Datetime,
            "price_lag_4h_source_time": pl.Datetime,
            "price_lag_1h_age": pl.Duration,
            "price_lag_4h_age": pl.Duration,
            "belief_shock_1h": pl.Float64,
            "belief_shock_4h": pl.Float64,
            "belief_shock_abs_1h": pl.Float64,
            "belief_shock_abs_4h": pl.Float64,
            "belief_shock_z_1h": pl.Float64,
            "spread": pl.Float64,
            "liquidity": pl.Float64,
            "volume": pl.Float64,
        }
    )
