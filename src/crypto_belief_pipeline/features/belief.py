from __future__ import annotations

import polars as pl

# TODO: For irregular production snapshots, replace simple .shift()-based lags
# with proper as-of time-window joins keyed off ``event_time - 1h`` /
# ``event_time - 4h``. The MVP assumes belief snapshots are at regular hourly
# resolution.

_BELIEF_SHOCK_STD_FLOOR = 0.01


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

    with_lags = sorted_df.with_columns(
        pl.col("price").shift(1).over(group_cols).alias("price_lag_1h"),
        pl.col("price").shift(4).over(group_cols).alias("price_lag_4h"),
    ).with_columns(
        (pl.col("direction") * (pl.col("price") - pl.col("price_lag_1h"))).alias("belief_shock_1h"),
        (pl.col("direction") * (pl.col("price") - pl.col("price_lag_4h"))).alias("belief_shock_4h"),
    )

    with_abs = with_lags.with_columns(
        pl.col("belief_shock_1h").abs().alias("belief_shock_abs_1h"),
        pl.col("belief_shock_4h").abs().alias("belief_shock_abs_4h"),
    )

    denom = pl.col("belief_shock_1h").std().over(group_cols).fill_null(_BELIEF_SHOCK_STD_FLOOR)
    floored_denom = (
        pl.when(denom < _BELIEF_SHOCK_STD_FLOOR).then(_BELIEF_SHOCK_STD_FLOOR).otherwise(denom)
    )
    with_z = with_abs.with_columns(
        (pl.col("belief_shock_1h") / floored_denom).alias("belief_shock_z_1h"),
    )

    out = with_z.rename({"timestamp": "event_time"}).select(
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
