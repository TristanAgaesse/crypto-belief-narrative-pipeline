from __future__ import annotations

import json
from datetime import UTC, datetime

import polars as pl

_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _parse_utc_ts(col: str, *, strict: bool = True) -> pl.Expr:
    parsed = pl.col(col).cast(pl.String).str.strptime(pl.Datetime, format=_TS_FORMAT, strict=strict)
    return parsed.dt.replace_time_zone("UTC")


def _parse_utc_ts_optional(col: str, *, out_name: str | None = None) -> pl.Expr:
    """Parse ISO Z timestamps; null or blank strings stay null (Gamma often omits end dates)."""

    name = out_name or col
    s = pl.col(col).cast(pl.String)
    parsed = s.str.strptime(pl.Datetime, format=_TS_FORMAT, strict=False)
    return (
        pl.when(s.is_null() | (s.str.strip_chars() == ""))
        .then(pl.lit(None, dtype=pl.Datetime(time_zone="UTC")))
        .otherwise(parsed.dt.replace_time_zone("UTC"))
    ).alias(name)


def _ingested_at_expr() -> pl.Expr:
    return pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("ingested_at")


def _raw_json(records: list[dict]) -> list[str]:
    """Serialize vendor lineage for the bronze ``raw_json`` column."""

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

    # Omit ``raw`` (full Gamma market dict per row) from ``pl.DataFrame(...)``. It is deep and
    # wide; Polars would build a heavy nested/struct column we never use in ``select``, while
    # bronze columns are only the flat contract fields plus ``raw_json`` for lineage.
    slim = [{k: v for k, v in r.items() if k != "raw"} for r in records]
    df = pl.DataFrame(slim).with_columns(pl.Series("raw_json", _raw_json(records)))

    bronze = df.select(
        pl.col("market_id").cast(pl.String),
        pl.col("question").cast(pl.String),
        pl.col("slug").cast(pl.String),
        pl.col("category").cast(pl.String),
        pl.col("active").cast(pl.Boolean),
        pl.col("closed").cast(pl.Boolean),
        _parse_utc_ts_optional("end_date"),
        pl.col("liquidity").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
        pl.col("raw_json").cast(pl.String),
    ).with_columns(_ingested_at_expr())

    bronze = bronze.with_columns(
        pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("load_timestamp")
    )

    bronze = bronze.sort(["market_id", "ingested_at"]).unique(subset=["market_id"], keep="last")
    return bronze


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

    # Same as ``normalize_markets``: keep lineage in ``raw_json`` only, not as a DataFrame column.
    slim = [{k: v for k, v in r.items() if k != "raw"} for r in records]
    df = pl.DataFrame(slim).with_columns(pl.Series("raw_json", _raw_json(records)))

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
        .with_columns(
            pl.when(pl.col("best_bid").is_not_null() & pl.col("best_ask").is_not_null())
            .then(pl.col("best_ask") - pl.col("best_bid"))
            .otherwise(None)
            .alias("spread")
        )
        .with_columns(_ingested_at_expr())
    )

    bronze = bronze.with_columns(
        pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("load_timestamp")
    )

    bronze = bronze.sort(["timestamp", "market_id", "outcome", "ingested_at"]).unique(
        subset=["timestamp", "market_id", "outcome"], keep="last"
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
        "load_timestamp",
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

    silver = bronze_prices.select(
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
        pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("processed_at"),
    )
    return silver.sort(["timestamp", "market_id", "outcome"]).unique(
        subset=["timestamp", "market_id", "outcome"], keep="last"
    )
