"""Rolling repricing and liquidity features for Kalshi (silver-level)."""

from __future__ import annotations

from typing import Any

import polars as pl

_TOLS: dict[str, Any] = {
    "5m": pl.duration(minutes=5),
    "15m": pl.duration(minutes=15),
    "1h": pl.duration(hours=1),
    "6h": pl.duration(hours=6),
    "24h": pl.duration(hours=24),
}


def _empty_features() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "as_of": pl.Datetime,
            "event_ticker": pl.String,
            "market_ticker": pl.String,
            "mapped_assets": pl.String,
            "mid_probability": pl.Float64,
            "delta_5m": pl.Float64,
            "delta_15m": pl.Float64,
            "delta_1h": pl.Float64,
            "delta_6h": pl.Float64,
            "delta_24h": pl.Float64,
            "velocity_1h": pl.Float64,
            "acceleration_1h": pl.Float64,
            "trade_confirm_score": pl.Float64,
            "liquidity_penalty": pl.Float64,
        }
    )


def _delta_column(sub: pl.DataFrame, label: str, tol: Any) -> pl.DataFrame:
    """Per-market: mid_now - mid at last snapshot at or before (as_of - tol)."""

    left = sub.select(
        pl.col("as_of"),
        pl.col("market_ticker"),
        pl.col("event_ticker"),
        pl.col("mid_probability").alias("mid_now"),
        pl.col("spread"),
    ).with_columns((pl.col("as_of") - tol).alias("_cut"))
    right = sub.select(
        pl.col("market_ticker"),
        pl.col("as_of").alias("_hist_as_of"),
        pl.col("mid_probability").alias("_mid_hist"),
    )
    left = left.sort(["market_ticker", "_cut"])
    right = right.sort(["market_ticker", "_hist_as_of"])
    merged = left.join_asof(
        right,
        left_on="_cut",
        right_on="_hist_as_of",
        strategy="backward",
        by="market_ticker",
    )
    return merged.with_columns(
        (pl.col("mid_now") - pl.col("_mid_hist")).alias(f"delta_{label}")
    ).select(
        [
            "as_of",
            "market_ticker",
            "event_ticker",
            "spread",
            "mid_now",
            f"delta_{label}",
        ]
    )


def build_event_repricing_features(
    snapshots: pl.DataFrame,
    trades: pl.DataFrame,
    markets: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling deltas and coarse trade/liquidity scores per market snapshot row."""

    if snapshots.height == 0:
        return _empty_features()

    snap = snapshots.sort(["market_ticker", "fetched_at"])
    snap = snap.rename({"fetched_at": "as_of"})
    if "mid_probability" not in snap.columns:
        snap = snap.with_columns(pl.lit(None).cast(pl.Float64).alias("mid_probability"))
    if "last_price" in snap.columns:
        snap = snap.with_columns(
            pl.when(pl.col("mid_probability").is_not_null())
            .then(pl.col("mid_probability"))
            .when(pl.col("last_price").is_not_null())
            .then(pl.col("last_price"))
            .otherwise(None)
            .alias("mid_probability"),
        )
    if "event_ticker" not in snap.columns or snap["event_ticker"].null_count() == snap.height:
        if markets.height and "event_ticker" in markets.columns:
            snap = snap.drop("event_ticker", strict=False)
            snap = snap.join(
                markets.select(["market_ticker", "event_ticker"]).unique(
                    subset=["market_ticker"], keep="last"
                ),
                on="market_ticker",
                how="left",
            )
    snap = snap.with_columns(
        pl.col("mid_probability").forward_fill().over("market_ticker"),
    )
    snap = snap.sort(["market_ticker", "as_of"])

    acc: pl.DataFrame | None = None
    for label, tol in _TOLS.items():
        part = _delta_column(snap, label, tol)
        if acc is None:
            acc = part
        else:
            acc = acc.join(
                part.select(["as_of", "market_ticker", f"delta_{label}"]),
                on=["as_of", "market_ticker"],
                how="left",
            )
    if acc is None:
        return _empty_features()

    acc = acc.rename({"mid_now": "mid_probability"})
    acc = acc.with_columns(
        (pl.col("delta_1h") - pl.col("delta_15m")).alias("velocity_1h"),
    ).with_columns(
        (pl.col("velocity_1h") - pl.col("velocity_1h").shift(1).over("market_ticker")).alias(
            "acceleration_1h"
        ),
    )

    if trades.height > 0 and "executed_at" in trades.columns:
        tr = trades.with_columns(
            pl.col("size_fp")
            .cast(pl.String)
            .str.replace_all(",", "")
            .cast(pl.Float64, strict=False)
            .fill_null(0.0)
            .alias("size"),
        )
        # Point-in-time: cumulative traded size only up to each snapshot `as_of`
        # (no lookahead from trades executed after the row's as_of).
        tr = tr.sort(["market_ticker", "executed_at"]).with_columns(
            pl.col("size").cum_sum().over("market_ticker").alias("_cum_trade_vol")
        )
        trade_asof = tr.select(
            [
                "market_ticker",
                pl.col("executed_at").alias("_trade_asof_key"),
                "_cum_trade_vol",
            ]
        ).sort(["market_ticker", "_trade_asof_key"])
        acc = acc.sort(["market_ticker", "as_of"]).join_asof(
            trade_asof,
            left_on="as_of",
            right_on="_trade_asof_key",
            by="market_ticker",
            strategy="backward",
        )
        acc = acc.drop("_trade_asof_key", strict=False)
        acc = acc.with_columns(
            (
                pl.col("_cum_trade_vol").fill_null(0.0) * pl.col("delta_5m").abs().fill_null(0.0)
            ).alias("trade_confirm_score")
        ).drop("_cum_trade_vol", strict=False)
    else:
        acc = acc.with_columns(pl.lit(0.0).alias("trade_confirm_score"))

    acc = acc.with_columns(
        pl.when(pl.col("spread").is_null() | (pl.col("spread") > 0.25))
        .then(pl.lit(1.0))
        .otherwise(pl.lit(0.0))
        .alias("liquidity_penalty"),
    )

    if markets.height and "mapped_assets" in markets.columns:
        acc = acc.join(
            markets.select(["market_ticker", "mapped_assets"]).unique(
                subset=["market_ticker"], keep="last"
            ),
            on="market_ticker",
            how="left",
        )
    else:
        acc = acc.with_columns(pl.lit("").alias("mapped_assets"))

    return acc.select(
        [
            pl.col("as_of"),
            pl.col("event_ticker"),
            pl.col("market_ticker"),
            pl.col("mapped_assets"),
            pl.col("mid_probability"),
            pl.col("delta_5m"),
            pl.col("delta_15m"),
            pl.col("delta_1h"),
            pl.col("delta_6h"),
            pl.col("delta_24h"),
            pl.col("velocity_1h"),
            pl.col("acceleration_1h"),
            pl.col("trade_confirm_score"),
            pl.col("liquidity_penalty"),
        ]
    )


__all__ = ["build_event_repricing_features"]
