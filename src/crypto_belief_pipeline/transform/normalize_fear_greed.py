from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import Any

import polars as pl


def _ingested_at_expr() -> pl.Expr:
    return pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("ingested_at")


def _load_timestamp_expr() -> pl.Expr:
    return pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("load_timestamp")


def _raw_json(records: list[dict]) -> list[str]:
    return [json.dumps(r.get("raw") or r, ensure_ascii=False) for r in records]


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_unix_ts_utc(value: Any) -> datetime | None:
    ts = _coerce_int(value)
    if ts is None:
        return None
    # Alternative.me timestamps are seconds since epoch
    return datetime.fromtimestamp(ts, tz=UTC).replace(microsecond=0)


def normalize_fear_greed_payload_records(raw_records: list[dict]) -> pl.DataFrame:
    """Normalize raw Fear & Greed staging/canonical records into typed bronze rows.

    Expects raw records shaped like `assets_raw.raw_fear_greed_*` writes:
      { source, fetched_at, ok, error, payload: { data: [ { value, value_classification, timestamp, ...}, ... ] } }
    """

    if not raw_records:
        return pl.DataFrame(
            schema={
                "source": pl.String,
                "index_name": pl.String,
                "timestamp_utc": pl.Datetime(time_zone="UTC"),
                "date_utc": pl.Date,
                "value": pl.Int64,
                "value_classification": pl.String,
                "time_until_update_seconds": pl.Int64,
                "fetched_at": pl.Datetime(time_zone="UTC"),
                "raw_json": pl.String,
                "ingested_at": pl.Datetime(time_zone="UTC"),
                "load_timestamp": pl.Datetime(time_zone="UTC"),
            }
        )

    rows: list[dict[str, Any]] = []
    for rec in raw_records:
        if not isinstance(rec, dict):
            continue
        payload = rec.get("payload")
        if not isinstance(payload, dict):
            continue
        data = payload.get("data") or []
        if not isinstance(data, list):
            continue

        fetched_at = rec.get("fetched_at")
        fetched_dt: datetime | None = None
        if isinstance(fetched_at, str) and fetched_at.strip():
            try:
                fetched_dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00")).astimezone(UTC)
            except ValueError:
                fetched_dt = None

        meta = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        time_until_update = meta.get("time_until_update") if isinstance(meta, dict) else None
        time_until_update_seconds = _coerce_int(time_until_update)

        for item in data:
            if not isinstance(item, dict):
                continue
            ts_dt = _parse_unix_ts_utc(item.get("timestamp"))
            if ts_dt is None:
                continue
            v = _coerce_int(item.get("value"))
            if v is None:
                continue
            rows.append(
                {
                    "source": str(rec.get("source") or "alternative_me"),
                    "index_name": "fear_and_greed",
                    "timestamp_utc": ts_dt,
                    "date_utc": ts_dt.date(),
                    "value": v,
                    "value_classification": (
                        str(item.get("value_classification")).strip()
                        if item.get("value_classification") is not None
                        else None
                    ),
                    "time_until_update_seconds": time_until_update_seconds,
                    "fetched_at": fetched_dt,
                    "raw": item,
                }
            )

    if not rows:
        return pl.DataFrame(
            schema={
                "source": pl.String,
                "index_name": pl.String,
                "timestamp_utc": pl.Datetime(time_zone="UTC"),
                "date_utc": pl.Date,
                "value": pl.Int64,
                "value_classification": pl.String,
                "time_until_update_seconds": pl.Int64,
                "fetched_at": pl.Datetime(time_zone="UTC"),
                "raw_json": pl.String,
                "ingested_at": pl.Datetime(time_zone="UTC"),
                "load_timestamp": pl.Datetime(time_zone="UTC"),
            }
        )

    slim = [{k: v for k, v in r.items() if k != "raw"} for r in rows]
    df = pl.DataFrame(slim).with_columns(pl.Series("raw_json", _raw_json(rows)))

    bronze = (
        df.select(
            pl.col("source").cast(pl.String),
            pl.col("index_name").cast(pl.String),
            pl.col("timestamp_utc").cast(pl.Datetime(time_zone="UTC")),
            pl.col("date_utc").cast(pl.Date),
            pl.col("value").cast(pl.Int64),
            pl.col("value_classification").cast(pl.String),
            pl.col("time_until_update_seconds").cast(pl.Int64),
            pl.col("fetched_at").cast(pl.Datetime(time_zone="UTC")),
            pl.col("raw_json").cast(pl.String),
        )
        .with_columns(_ingested_at_expr())
        .with_columns(_load_timestamp_expr())
    )

    # Deterministic dedupe: keep the most recently fetched row for each day.
    bronze = bronze.sort(["source", "date_utc", "fetched_at", "load_timestamp"]).unique(
        subset=["source", "date_utc"], keep="last"
    )
    return bronze


def to_fear_greed_daily(bronze: pl.DataFrame) -> pl.DataFrame:
    if bronze.is_empty():
        return pl.DataFrame(
            schema={
                "source": pl.String,
                "date_utc": pl.Date,
                "value": pl.Int64,
                "value_classification": pl.String,
                "time_until_update_seconds": pl.Int64,
                "first_seen_at": pl.Datetime(time_zone="UTC"),
                "last_seen_at": pl.Datetime(time_zone="UTC"),
                "processed_at": pl.Datetime(time_zone="UTC"),
            }
        )

    # First/last seen are tracked within the recomputed canonical partition by fetched_at.
    daily = (
        bronze.sort(["source", "date_utc", "fetched_at"])
        .group_by(["source", "date_utc"])
        .agg(
            pl.col("value").last().alias("value"),
            pl.col("value_classification").last().alias("value_classification"),
            pl.col("time_until_update_seconds").last().alias("time_until_update_seconds"),
            pl.col("fetched_at").first().alias("first_seen_at"),
            pl.col("fetched_at").last().alias("last_seen_at"),
        )
        .with_columns(pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("processed_at"))
    )
    return daily.sort(["source", "date_utc"]).unique(subset=["source", "date_utc"], keep="last")


def build_regime_features(daily: pl.DataFrame) -> pl.DataFrame:
    if daily.is_empty():
        return pl.DataFrame(
            schema={
                "source": pl.String,
                "date_utc": pl.Date,
                "value": pl.Int64,
                "value_classification": pl.String,
                "chg_1d": pl.Float64,
                "chg_3d": pl.Float64,
                "chg_7d": pl.Float64,
                "ma_7d": pl.Float64,
                "ma_14d": pl.Float64,
                "ma_30d": pl.Float64,
                "zscore_30d": pl.Float64,
                "is_extreme_fear": pl.Boolean,
                "is_fear": pl.Boolean,
                "is_neutral": pl.Boolean,
                "is_greed": pl.Boolean,
                "is_extreme_greed": pl.Boolean,
                "risk_on_score": pl.Float64,
                "processed_at": pl.Datetime(time_zone="UTC"),
            }
        )

    df = daily.sort(["source", "date_utc"]).with_columns(pl.col("value").cast(pl.Float64))

    def _rolling_mean(n: int) -> pl.Expr:
        return pl.col("value").rolling_mean(window_size=n, min_periods=max(1, n // 2)).over("source")

    def _rolling_std(n: int) -> pl.Expr:
        return pl.col("value").rolling_std(window_size=n, min_periods=max(2, n // 2)).over("source")

    feats = df.with_columns(
        (pl.col("value") - pl.col("value").shift(1)).over("source").alias("chg_1d"),
        (pl.col("value") - pl.col("value").shift(3)).over("source").alias("chg_3d"),
        (pl.col("value") - pl.col("value").shift(7)).over("source").alias("chg_7d"),
        _rolling_mean(7).alias("ma_7d"),
        _rolling_mean(14).alias("ma_14d"),
        _rolling_mean(30).alias("ma_30d"),
    ).with_columns(
        (
            (pl.col("value") - pl.col("ma_30d"))
            / pl.when(_rolling_std(30) == 0).then(None).otherwise(_rolling_std(30))
        ).alias("zscore_30d")
    )

    cls = pl.col("value_classification").cast(pl.String)
    feats = feats.with_columns(
        (cls == pl.lit("Extreme Fear")).alias("is_extreme_fear"),
        (cls == pl.lit("Fear")).alias("is_fear"),
        (cls == pl.lit("Neutral")).alias("is_neutral"),
        (cls == pl.lit("Greed")).alias("is_greed"),
        (cls == pl.lit("Extreme Greed")).alias("is_extreme_greed"),
    )

    # Simple monotone score: map index range [0,100] to [-1, +1]
    feats = feats.with_columns(((pl.col("value") / 50.0) - 1.0).alias("risk_on_score"))
    feats = feats.with_columns(pl.lit(datetime.now(UTC).replace(microsecond=0)).alias("processed_at"))

    return feats.select(
        "source",
        "date_utc",
        pl.col("value").cast(pl.Int64).alias("value"),
        "value_classification",
        "chg_1d",
        "chg_3d",
        "chg_7d",
        "ma_7d",
        "ma_14d",
        "ma_30d",
        "zscore_30d",
        "is_extreme_fear",
        "is_fear",
        "is_neutral",
        "is_greed",
        "is_extreme_greed",
        "risk_on_score",
        "processed_at",
    )


__all__ = [
    "build_regime_features",
    "normalize_fear_greed_payload_records",
    "to_fear_greed_daily",
]

