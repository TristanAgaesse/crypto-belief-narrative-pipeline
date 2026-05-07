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
    """Serialize vendor lineage for the bronze ``raw_json`` column."""

    return [json.dumps(r.get("raw") or r, ensure_ascii=False) for r in records]


def normalize_timeline(records: list[dict]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime,
                "narrative": pl.String,
                "query": pl.String,
                "mention_volume": pl.Float64,
                "avg_tone": pl.Float64,
                "source": pl.String,
                "raw_json": pl.String,
                "ingested_at": pl.Datetime,
            }
        )

    # Omit ``raw`` (GDELT row dict) from the frame for the same pattern as other providers:
    # bronze reads only the flat Step-2 fields; full lineage is stored once in ``raw_json``.
    # Avoids an extra struct column and keeps frame construction aligned with Polymarket/Binance.
    slim = [{k: v for k, v in r.items() if k != "raw"} for r in records]
    df = pl.DataFrame(slim).with_columns(pl.Series("raw_json", _raw_json(records)))

    bronze = df.select(
        _parse_utc_ts("timestamp").alias("timestamp"),
        pl.col("narrative").cast(pl.String),
        pl.col("query").cast(pl.String),
        pl.col("mention_volume").cast(pl.Float64),
        pl.col("avg_tone").cast(pl.Float64),
        pl.col("source").cast(pl.String),
        pl.col("raw_json").cast(pl.String),
    ).with_columns(_ingested_at_expr())

    bronze = bronze.with_columns(
        pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("load_timestamp")
    )

    bronze = (
        bronze.sort(["timestamp", "narrative", "query", "source", "ingested_at"])
        .unique(subset=["timestamp", "narrative", "query", "source"], keep="last")
    )

    return bronze.select(
        "timestamp",
        "narrative",
        "query",
        "mention_volume",
        "avg_tone",
        "source",
        "raw_json",
        "ingested_at",
        "load_timestamp",
    )


def to_narrative_counts(bronze_timeline: pl.DataFrame) -> pl.DataFrame:
    if bronze_timeline.is_empty():
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime,
                "narrative": pl.String,
                "query": pl.String,
                "mention_volume": pl.Float64,
                "avg_tone": pl.Float64,
                "source": pl.String,
            }
        )

    silver = bronze_timeline.select(
        "timestamp",
        "narrative",
        "query",
        "mention_volume",
        "avg_tone",
        "source",
        pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("processed_at"),
    )
    return (
        silver.sort(["timestamp", "narrative", "query", "source"])
        .unique(subset=["timestamp", "narrative", "query", "source"], keep="last")
    )
