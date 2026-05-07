from __future__ import annotations

import polars as pl


def add_directional_labels(df: pl.DataFrame) -> pl.DataFrame:
    """Multiply return columns by ``direction`` to get directional labels.

    Expects the input frame to already contain ``direction`` plus the relevant
    return columns. Missing source columns are tolerated by emitting nulls so
    this function can run on partially joined intermediates during development.
    """

    direction = pl.col("direction")
    additions: list[pl.Expr] = []

    if "asset_ret_past_1h" in df.columns:
        additions.append(
            (direction * pl.col("asset_ret_past_1h")).alias("directional_price_reaction_1h")
        )
    if "asset_ret_past_4h" in df.columns:
        additions.append(
            (direction * pl.col("asset_ret_past_4h")).alias("directional_price_reaction_4h")
        )
    if "future_ret_1h" in df.columns:
        additions.append((direction * pl.col("future_ret_1h")).alias("directional_future_ret_1h"))
    if "future_ret_4h" in df.columns:
        additions.append((direction * pl.col("future_ret_4h")).alias("directional_future_ret_4h"))
    if "future_ret_24h" in df.columns:
        additions.append((direction * pl.col("future_ret_24h")).alias("directional_future_ret_24h"))

    if not additions:
        return df

    return df.with_columns(additions)
