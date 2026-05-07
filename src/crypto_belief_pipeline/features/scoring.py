from __future__ import annotations

import polars as pl

_BELIEF_SHOCK_THRESHOLD = 0.08
_CONFIDENCE_THRESHOLD = 0.6
_RELEVANT_LEVELS = ("medium", "high")


def add_penalties(df: pl.DataFrame) -> pl.DataFrame:
    """Add ``spread_penalty`` and ``illiquidity_penalty`` columns."""

    spread_penalty = (
        pl.when(pl.col("spread").is_null())
        .then(0.0)
        .otherwise(
            pl.min_horizontal(pl.col("spread") / 0.10, pl.lit(1.0)),
        )
        .alias("spread_penalty")
    )

    illiquidity_penalty = (
        pl.when(pl.col("liquidity").is_null() | (pl.col("liquidity") <= 0))
        .then(1.0)
        .when(pl.col("liquidity") < 1000)
        .then(0.5)
        .otherwise(0.0)
        .alias("illiquidity_penalty")
    )

    return df.with_columns(spread_penalty, illiquidity_penalty)


def add_underreaction_score(df: pl.DataFrame) -> pl.DataFrame:
    """Add the ``underreaction_score`` column.

    underreaction_score =
        2.0 * belief_shock_abs_1h
      + 1.0 * max(narrative_z_1h, 0)
      - 1.5 * max(directional_price_reaction_1h, 0)
      - 1.0 * spread_penalty
      - 1.0 * illiquidity_penalty

    Missing inputs are treated as zero so partially joined rows still receive a
    finite score.
    """

    abs_shock = pl.col("belief_shock_abs_1h").fill_null(0.0)
    narr_z_pos = pl.max_horizontal(pl.col("narrative_z_1h").fill_null(0.0), pl.lit(0.0))
    directional_pos = pl.max_horizontal(
        pl.col("directional_price_reaction_1h").fill_null(0.0), pl.lit(0.0)
    )
    spread_pen = pl.col("spread_penalty").fill_null(0.0)
    illiq_pen = pl.col("illiquidity_penalty").fill_null(0.0)

    score = (
        2.0 * abs_shock
        + 1.0 * narr_z_pos
        - 1.5 * directional_pos
        - 1.0 * spread_pen
        - 1.0 * illiq_pen
    ).alias("underreaction_score")

    return df.with_columns(score)


def add_candidate_flag(df: pl.DataFrame) -> pl.DataFrame:
    """Add ``is_candidate_event`` and ``quality_flags`` columns.

    Candidate rule:
      belief_shock_abs_1h >= 0.08
      AND underreaction_score > 0
      AND confidence >= 0.6
      AND relevance in {"medium", "high"}

    Quality flags (joined with "|"):
      - "missing_future_labels" if all of future_ret_{1h,4h,24h} are null.
      - "missing_narrative" if narrative_z_1h is null.
      - "missing_price_lag" if asset_ret_past_1h is null.
    """

    is_candidate = (
        (pl.col("belief_shock_abs_1h").fill_null(0.0) >= _BELIEF_SHOCK_THRESHOLD)
        & (pl.col("underreaction_score").fill_null(0.0) > 0.0)
        & (pl.col("confidence").fill_null(0.0) >= _CONFIDENCE_THRESHOLD)
        & (pl.col("relevance").is_in(list(_RELEVANT_LEVELS)))
    ).alias("is_candidate_event")

    missing_future_labels = (
        pl.col("future_ret_1h").is_null()
        & pl.col("future_ret_4h").is_null()
        & pl.col("future_ret_24h").is_null()
    )

    flags_struct = pl.struct(
        [
            pl.when(missing_future_labels)
            .then(pl.lit("missing_future_labels"))
            .otherwise(pl.lit(""))
            .alias("a"),
            pl.when(pl.col("narrative_z_1h").is_null())
            .then(pl.lit("missing_narrative"))
            .otherwise(pl.lit(""))
            .alias("b"),
            pl.when(pl.col("asset_ret_past_1h").is_null())
            .then(pl.lit("missing_price_lag"))
            .otherwise(pl.lit(""))
            .alias("c"),
        ]
    )

    df = df.with_columns(is_candidate)
    df = df.with_columns(flags_struct.alias("_quality_flags_struct"))

    quality_flags = df["_quality_flags_struct"].map_elements(_join_flags, return_dtype=pl.String)

    return df.with_columns(quality_flags.alias("quality_flags")).drop("_quality_flags_struct")


def _join_flags(flags: dict[str, str]) -> str:
    parts = [v for v in flags.values() if v]
    return "|".join(parts)
