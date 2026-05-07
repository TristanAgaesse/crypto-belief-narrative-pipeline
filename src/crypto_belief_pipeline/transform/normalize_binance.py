from __future__ import annotations

import json
from datetime import UTC, datetime

import polars as pl

_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

_SYMBOL_TO_ASSET: dict[str, str] = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "SOLUSDT": "SOL",
}


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
    """Serialize vendor lineage for the bronze ``raw_json`` column."""

    return [json.dumps(r.get("raw") or r, ensure_ascii=False) for r in records]


def normalize_klines(records: list[dict]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime,
                "exchange": pl.String,
                "symbol": pl.String,
                "interval": pl.String,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "quote_volume": pl.Float64,
                "number_of_trades": pl.Int64,
                "raw_json": pl.String,
                "ingested_at": pl.Datetime,
            }
        )

    # Step-2 records include ``raw``: the exact kline array returned by Binance (ints for
    # times, strings for prices/volumes). Polars cannot infer a single dtype for that column
    # (it tries e.g. Int64, then fails on the first string). Bronze only needs the typed
    # fields we already parse in the collector; lineage lives in ``raw_json`` via ``_raw_json``.
    slim = [{k: v for k, v in r.items() if k != "raw"} for r in records]
    df = pl.DataFrame(slim).with_columns(pl.Series("raw_json", _raw_json(records)))

    bronze = df.select(
        _parse_utc_ts("open_time").alias("timestamp"),
        pl.lit("binance_usdm").alias("exchange"),
        pl.col("symbol").cast(pl.String),
        pl.col("interval").cast(pl.String),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
        pl.col("quote_volume").cast(pl.Float64),
        pl.col("number_of_trades").cast(pl.Int64),
        pl.col("raw_json").cast(pl.String),
    ).with_columns(_ingested_at_expr())

    bronze = bronze.with_columns(
        pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("load_timestamp")
    )

    # Deterministic within-batch dedup (overlap/retries can produce duplicates).
    bronze = bronze.sort(["timestamp", "symbol", "interval", "ingested_at"]).unique(
        subset=["timestamp", "symbol", "interval"], keep="last"
    )

    return bronze.select(
        "timestamp",
        "exchange",
        "symbol",
        "interval",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "number_of_trades",
        "raw_json",
        "ingested_at",
        "load_timestamp",
    )


def to_crypto_candles_1m(bronze_klines: pl.DataFrame) -> pl.DataFrame:
    if bronze_klines.is_empty():
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime,
                "exchange": pl.String,
                "asset": pl.String,
                "symbol": pl.String,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "quote_volume": pl.Float64,
                "number_of_trades": pl.Int64,
            }
        )

    silver = bronze_klines.select(
        pl.col("timestamp"),
        pl.col("exchange"),
        pl.col("symbol").replace_strict(_SYMBOL_TO_ASSET, default=None).alias("asset"),
        pl.col("symbol"),
        pl.col("open"),
        pl.col("high"),
        pl.col("low"),
        pl.col("close"),
        pl.col("volume"),
        pl.col("quote_volume"),
        pl.col("number_of_trades"),
        pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("processed_at"),
    )
    return silver.sort(["timestamp", "asset", "symbol"]).unique(
        subset=["timestamp", "asset"], keep="last"
    )
