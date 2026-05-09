from __future__ import annotations

from datetime import timedelta

import polars as pl

_FEATURE_JOIN_TOLERANCE = timedelta(days=14)


def _ensure_risk_on_score_column(df: pl.DataFrame) -> pl.DataFrame:
    """Attach ``risk_on_score`` when the regime table or join did not supply it."""

    if "risk_on_score" in df.columns:
        return df
    return df.with_columns(pl.lit(None, dtype=pl.Float64).alias("risk_on_score"))


def join_fear_greed_asof(events: pl.DataFrame, *, fear_greed_regime: pl.DataFrame) -> pl.DataFrame:
    """Causal as-of join from event_time to last known Fear & Greed regime row.

    Fear & Greed is daily-granularity; we align by `date_utc` and join backward
    so events never see future regime values.

    **Contract:** every return path yields a frame that includes ``risk_on_score``
    (``Float64``, possibly all-null). Gold assumes this column exists once this
    join has run, including when ``events`` has zero rows (no belief rows for the
    partition is valid; skipping the join previously dropped the column entirely).
    """

    if events.is_empty():
        return _ensure_risk_on_score_column(events)
    if fear_greed_regime.is_empty():
        return _ensure_risk_on_score_column(events)

    if "event_time" not in events.columns:
        return _ensure_risk_on_score_column(events)
    if "date_utc" not in fear_greed_regime.columns:
        return _ensure_risk_on_score_column(events)

    left = events.with_columns(pl.col("event_time").dt.truncate("1d").alias("_event_day"))
    right = fear_greed_regime.with_columns(
        pl.col("date_utc").cast(pl.Datetime).dt.replace_time_zone("UTC").alias("_event_day")
    )

    # Use a deterministic single-source view (Alternative.me) if present.
    if "source" in right.columns:
        right = right.filter(pl.col("source") == pl.lit("alternative_me"))

    right = right.sort(["_event_day"])
    if right.is_empty():
        return _ensure_risk_on_score_column(left.drop("_event_day"))

    # Join on date with a bounded tolerance (avoid accidental far-back fills on sparse backtests).
    joined = left.sort(["_event_day"]).join_asof(
        right,
        on="_event_day",
        strategy="backward",
        tolerance=_FEATURE_JOIN_TOLERANCE,
    )

    out = joined.drop("_event_day")
    return _ensure_risk_on_score_column(out)


__all__ = ["join_fear_greed_asof"]
