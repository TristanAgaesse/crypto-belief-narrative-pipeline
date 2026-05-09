from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from crypto_belief_pipeline.contracts import GOLD_LIVE_SIGNALS, GOLD_TRAINING_EXAMPLES
from crypto_belief_pipeline.features.belief import build_belief_features
from crypto_belief_pipeline.features.fear_greed import join_fear_greed_asof
from crypto_belief_pipeline.features.labels import add_directional_labels
from crypto_belief_pipeline.features.market_tags import (
    DEFAULT_MARKET_TAGS_PATH,
    load_market_tags,
    validate_market_tags,
)
from crypto_belief_pipeline.features.narrative import build_narrative_features
from crypto_belief_pipeline.features.prices import build_price_features
from crypto_belief_pipeline.features.scoring import (
    add_candidate_flag,
    add_penalties,
    add_underreaction_score,
)
from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.read import (
    LakeKeyNotFound,
    read_parquet_df,
    read_parquet_partition_df,
)
from crypto_belief_pipeline.lake.write import write_parquet_df

_PRICE_FEATURE_JOIN_TOLERANCE = timedelta(minutes=2)


def _safe_partition_slug(partition_key: str) -> str:
    return partition_key.replace(":", "-")


def _partition_parquet_key(
    layer: str, dataset: str, run_date: date | str, partition_key: str
) -> str:
    prefix = partition_path(layer, dataset, run_date)
    return f"{prefix}/partition={_safe_partition_slug(partition_key)}/data.parquet"


def _normalize_event_time_utc(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure event_time is consistently timezone-aware UTC for joins.

    Live Parquet inputs can yield tz-aware datetimes while some transforms emit tz-naive
    datetimes. Polars requires join keys to match exactly.
    """

    if "event_time" not in df.columns:
        return df
    dtype = df.schema.get("event_time")
    tz = getattr(dtype, "time_zone", None)
    if tz is None:
        return df.with_columns(pl.col("event_time").dt.replace_time_zone("UTC"))
    return df.with_columns(pl.col("event_time").dt.convert_time_zone("UTC"))


def _join_price_features_asof(events: pl.DataFrame, price_features: pl.DataFrame) -> pl.DataFrame:
    """Attach price features using the latest candle feature near each event time."""

    if events.is_empty() or price_features.is_empty():
        return events.join(price_features, on=["event_time", "asset"], how="left")

    right = price_features.with_columns(
        pl.col("event_time").alias("price_feature_source_time")
    ).sort(["asset", "event_time"])
    return (
        events.sort(["asset", "event_time"])
        .join_asof(
            right,
            on="event_time",
            by="asset",
            strategy="backward",
            tolerance=_PRICE_FEATURE_JOIN_TOLERANCE,
        )
        .with_columns(
            (pl.col("event_time") - pl.col("price_feature_source_time")).alias("price_feature_age")
        )
    )


def _gold_keys(run_date: date | str, partition_key: str | None = None) -> tuple[str, str]:
    if partition_key:
        return (
            _partition_parquet_key("gold", "training_examples", run_date, partition_key),
            _partition_parquet_key("gold", "live_signals", run_date, partition_key),
        )
    training_prefix = partition_path("gold", "training_examples", run_date)
    live_prefix = partition_path("gold", "live_signals", run_date)
    return f"{training_prefix}/data.parquet", f"{live_prefix}/data.parquet"


def _partition_window_from_key(partition_key: str) -> tuple[datetime, datetime]:
    start = datetime.strptime(partition_key, "%Y-%m-%d-%H:%M").replace(tzinfo=UTC)
    return start, start + timedelta(hours=1)


def _partition_scoped_fallback_df(
    *,
    partition_prefix: str,
    partition_key: str,
    bucket: str | None,
    allow_empty_when_missing: bool = False,
) -> pl.DataFrame:
    """Fallback read for partitioned silver: scan prefix, then keep this hour only.

    When the date prefix has no Parquet at all, :func:`read_parquet_partition_df`
    returns a zero-width frame. Required inputs (belief, candles) still raise
    :class:`LakeKeyNotFound`; optional narrative silver may return an empty frame
    so gold matches the non-partitioned ``read_parquet_partition_df`` behavior.
    """
    df = read_parquet_partition_df(partition_prefix, bucket=bucket)
    if df.width == 0:
        if allow_empty_when_missing:
            return pl.DataFrame()
        slug = _safe_partition_slug(partition_key)
        raise LakeKeyNotFound(
            f"lake key not found: {partition_prefix}/partition={slug}/data.parquet"
        )
    if "timestamp" not in df.columns:
        raise ValueError(
            f"partition fallback requires `timestamp` column for hour scoping: {partition_prefix}"
        )
    window_start, window_end = _partition_window_from_key(partition_key)
    return df.filter(
        (pl.col("timestamp") >= pl.lit(window_start)) & (pl.col("timestamp") < pl.lit(window_end))
    )


def _read_partitioned_or_fallback(
    *,
    explicit_key: str | None,
    canonical_key: str,
    partition_prefix: str,
    partition_key: str,
    bucket: str | None,
    allow_empty_when_missing: bool = False,
) -> pl.DataFrame:
    if explicit_key is not None:
        return read_parquet_df(explicit_key, bucket=bucket)
    try:
        return read_parquet_df(canonical_key, bucket=bucket)
    except LakeKeyNotFound:
        return _partition_scoped_fallback_df(
            partition_prefix=partition_prefix,
            partition_key=partition_key,
            bucket=bucket,
            allow_empty_when_missing=allow_empty_when_missing,
        )


def build_gold_tables(
    run_date: date | str,
    partition_key: str | None = None,
    belief_key: str | None = None,
    candles_key: str | None = None,
    narrative_key: str | None = None,
    fear_greed_regime_key: str | None = None,
    market_tags_path: str | Path = DEFAULT_MARKET_TAGS_PATH,
    *,
    bucket: str | None = None,
) -> dict[str, str]:
    """Build the Step 4 gold tables and write them to S3.

    Reads silver Parquet inputs, joins belief / narrative / price features,
    applies directional labels and the underreaction scoring, then writes the
    full ``training_examples`` table and the filtered ``live_signals`` table.

    ``bucket`` overrides the default lake bucket for both reads and writes.
    Sample-mode callers pass the dedicated sample bucket so silver inputs and
    gold outputs stay co-located there.

    Returns a dict mapping ``{"training_examples": key, "live_signals": key}``.

    When a silver key is omitted, reads the full partition from the lake (single
    ``data.parquet`` and/or Dagster microbatch shards under ``date=...``).
    When provided (e.g. CLI threading exact outputs), reads that concrete object key.
    """

    if partition_key:
        belief_partition_prefix = partition_path("silver", "belief_price_snapshots", run_date)
        candles_partition_prefix = partition_path("silver", "crypto_candles_1m", run_date)
        narrative_partition_prefix = partition_path("silver", "narrative_counts", run_date)
        belief_fallback = _partition_parquet_key(
            "silver", "belief_price_snapshots", run_date, partition_key
        )
        candles_fallback = _partition_parquet_key(
            "silver", "crypto_candles_1m", run_date, partition_key
        )
        narrative_fallback = _partition_parquet_key(
            "silver", "narrative_counts", run_date, partition_key
        )
        fear_greed_fallback = _partition_parquet_key(
            "silver", "fear_greed_regime_features", run_date, partition_key
        )
        belief_silver = _read_partitioned_or_fallback(
            explicit_key=belief_key,
            canonical_key=belief_fallback,
            partition_prefix=belief_partition_prefix,
            partition_key=partition_key,
            bucket=bucket,
        )
        candles_silver = _read_partitioned_or_fallback(
            explicit_key=candles_key,
            canonical_key=candles_fallback,
            partition_prefix=candles_partition_prefix,
            partition_key=partition_key,
            bucket=bucket,
        )
        narrative_silver = _read_partitioned_or_fallback(
            explicit_key=narrative_key,
            canonical_key=narrative_fallback,
            partition_prefix=narrative_partition_prefix,
            partition_key=partition_key,
            bucket=bucket,
            allow_empty_when_missing=True,
        )
        try:
            fear_greed_regime = read_parquet_df(
                fear_greed_regime_key or fear_greed_fallback, bucket=bucket
            )
        except LakeKeyNotFound:
            fear_greed_regime = read_parquet_partition_df(
                partition_path("silver", "fear_greed_regime_features", run_date), bucket=bucket
            )
    else:
        belief_silver = (
            read_parquet_df(belief_key, bucket=bucket)
            if belief_key is not None
            else read_parquet_partition_df(
                partition_path("silver", "belief_price_snapshots", run_date), bucket=bucket
            )
        )
        candles_silver = (
            read_parquet_df(candles_key, bucket=bucket)
            if candles_key is not None
            else read_parquet_partition_df(
                partition_path("silver", "crypto_candles_1m", run_date), bucket=bucket
            )
        )
        narrative_silver = (
            read_parquet_df(narrative_key, bucket=bucket)
            if narrative_key is not None
            else read_parquet_partition_df(
                partition_path("silver", "narrative_counts", run_date), bucket=bucket
            )
        )
        fear_greed_regime = (
            read_parquet_df(fear_greed_regime_key, bucket=bucket)
            if fear_greed_regime_key is not None
            else read_parquet_partition_df(
                partition_path("silver", "fear_greed_regime_features", run_date), bucket=bucket
            )
        )

    tags = validate_market_tags(load_market_tags(market_tags_path))

    belief_features = build_belief_features(belief_silver, tags)
    narrative_features = build_narrative_features(narrative_silver)
    price_features = build_price_features(candles_silver)

    belief_features = _normalize_event_time_utc(belief_features)
    narrative_features = _normalize_event_time_utc(narrative_features)
    price_features = _normalize_event_time_utc(price_features)

    # Deterministic replay safety: collapse duplicated rows before joins.
    belief_subset = ["event_time", "market_id"]
    if "outcome" in belief_features.columns:
        belief_subset.append("outcome")
    belief_features = belief_features.unique(subset=belief_subset, keep="last")

    price_subset = ["event_time", "asset"]
    if all(c in price_features.columns for c in price_subset):
        price_features = price_features.unique(subset=price_subset, keep="last")

    narrative_subset = ["event_time", "narrative"]
    if all(c in narrative_features.columns for c in narrative_subset):
        narrative_features = narrative_features.unique(subset=narrative_subset, keep="last")

    joined = belief_features.join(narrative_features, on=["event_time", "narrative"], how="left")
    joined = _join_price_features_asof(joined, price_features)
    joined = join_fear_greed_asof(joined, fear_greed_regime=fear_greed_regime)

    with_labels = add_directional_labels(joined)
    with_pen = add_penalties(with_labels)
    with_score = add_underreaction_score(with_pen)
    training_examples = add_candidate_flag(with_score)

    live_signals = training_examples.filter(pl.col("is_candidate_event"))

    training_key, live_key = _gold_keys(run_date, partition_key)

    GOLD_TRAINING_EXAMPLES.validate(training_examples)
    GOLD_LIVE_SIGNALS.validate(live_signals)

    write_parquet_df(training_examples, training_key, bucket=bucket)
    write_parquet_df(live_signals, live_key, bucket=bucket)

    return {
        "training_examples": training_key,
        "live_signals": live_key,
    }
