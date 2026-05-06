from __future__ import annotations

import json
from datetime import UTC, datetime

import polars as pl

_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _parse_utc_ts(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.String)
        .str.strptime(pl.Datetime, format=_TS_FORMAT, strict=True)
        .dt.replace_time_zone("UTC")
    )


def _ingested_at_expr() -> pl.Expr:
    return pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("ingested_at")


def _raw_json(records: list[dict]) -> list[str]:
    return [json.dumps(r.get("raw") or r, ensure_ascii=False) for r in records]


def normalize_markets(records: list[dict]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame(
            schema={
                "market_id": pl.String,
                "question": pl.String,
                "slug": pl.String,
                "category": pl.String,
                "active": pl.Boolean,
                "closed": pl.Boolean,
                "end_date": pl.Datetime,
                "liquidity": pl.Float64,
                "volume": pl.Float64,
                "raw_json": pl.String,
                "ingested_at": pl.Datetime,
            }
        )

    df = pl.DataFrame(records).with_columns(pl.Series("raw_json", _raw_json(records)))

    return df.select(
        pl.col("market_id").cast(pl.String),
        pl.col("question").cast(pl.String),
        pl.col("slug").cast(pl.String),
        pl.col("category").cast(pl.String),
        pl.col("active").cast(pl.Boolean),
        pl.col("closed").cast(pl.Boolean),
        _parse_utc_ts("end_date").alias("end_date"),
        pl.col("liquidity").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
        pl.col("raw_json").cast(pl.String),
    ).with_columns(_ingested_at_expr())


def normalize_price_snapshots(records: list[dict]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime,
                "market_id": pl.String,
                "outcome": pl.String,
                "price": pl.Float64,
                "best_bid": pl.Float64,
                "best_ask": pl.Float64,
                "spread": pl.Float64,
                "liquidity": pl.Float64,
                "volume": pl.Float64,
                "raw_json": pl.String,
                "ingested_at": pl.Datetime,
            }
        )

    df = pl.DataFrame(records).with_columns(pl.Series("raw_json", _raw_json(records)))

    bronze = (
        df.select(
            _parse_utc_ts("timestamp").alias("timestamp"),
            pl.col("market_id").cast(pl.String),
            pl.col("outcome").cast(pl.String),
            pl.col("price").cast(pl.Float64),
            pl.col("best_bid").cast(pl.Float64),
            pl.col("best_ask").cast(pl.Float64),
            pl.col("liquidity").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
            pl.col("raw_json").cast(pl.String),
        )
        .with_columns((pl.col("best_ask") - pl.col("best_bid")).alias("spread"))
        .with_columns(_ingested_at_expr())
    )

    return bronze.select(
        "timestamp",
        "market_id",
        "outcome",
        "price",
        "best_bid",
        "best_ask",
        "spread",
        "liquidity",
        "volume",
        "raw_json",
        "ingested_at",
    )


def to_belief_price_snapshots(bronze_prices: pl.DataFrame) -> pl.DataFrame:
    if bronze_prices.is_empty():
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime,
                "platform": pl.String,
                "market_id": pl.String,
                "outcome": pl.String,
                "price": pl.Float64,
                "best_bid": pl.Float64,
                "best_ask": pl.Float64,
                "spread": pl.Float64,
                "liquidity": pl.Float64,
                "volume": pl.Float64,
            }
        )

    return bronze_prices.select(
        pl.col("timestamp"),
        pl.lit("polymarket").alias("platform"),
        pl.col("market_id"),
        pl.col("outcome"),
        pl.col("price"),
        pl.col("best_bid"),
        pl.col("best_ask"),
        pl.col("spread"),
        pl.col("liquidity"),
        pl.col("volume"),
    )
