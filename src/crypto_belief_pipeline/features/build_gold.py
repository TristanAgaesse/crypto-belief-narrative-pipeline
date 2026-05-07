from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from crypto_belief_pipeline.features.belief import build_belief_features
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
from crypto_belief_pipeline.lake.read import read_parquet_df
from crypto_belief_pipeline.lake.write import write_parquet_df


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


def _default_silver_keys(run_date: date | str) -> tuple[str, str, str]:
    belief_prefix = partition_path("silver", "belief_price_snapshots", run_date)
    candles_prefix = partition_path("silver", "crypto_candles_1m", run_date)
    narrative_prefix = partition_path("silver", "narrative_counts", run_date)
    return (
        f"{belief_prefix}/data.parquet",
        f"{candles_prefix}/data.parquet",
        f"{narrative_prefix}/data.parquet",
    )


def _gold_keys(run_date: date | str) -> tuple[str, str]:
    training_prefix = partition_path("gold", "training_examples", run_date)
    live_prefix = partition_path("gold", "live_signals", run_date)
    return f"{training_prefix}/data.parquet", f"{live_prefix}/data.parquet"


def build_gold_tables(
    run_date: date | str,
    belief_key: str | None = None,
    candles_key: str | None = None,
    narrative_key: str | None = None,
    market_tags_path: str | Path = DEFAULT_MARKET_TAGS_PATH,
) -> dict[str, str]:
    """Build the Step 4 gold tables and write them to S3.

    Reads silver Parquet inputs, joins belief / narrative / price features,
    applies directional labels and the underreaction scoring, then writes the
    full ``training_examples`` table and the filtered ``live_signals`` table.

    Returns a dict mapping ``{"training_examples": key, "live_signals": key}``.
    """

    default_belief, default_candles, default_narrative = _default_silver_keys(run_date)
    belief_key = belief_key or default_belief
    candles_key = candles_key or default_candles
    narrative_key = narrative_key or default_narrative

    belief_silver = read_parquet_df(belief_key)
    candles_silver = read_parquet_df(candles_key)
    narrative_silver = read_parquet_df(narrative_key)

    tags = validate_market_tags(load_market_tags(market_tags_path))

    belief_features = build_belief_features(belief_silver, tags)
    narrative_features = build_narrative_features(narrative_silver)
    price_features = build_price_features(candles_silver)

    belief_features = _normalize_event_time_utc(belief_features)
    narrative_features = _normalize_event_time_utc(narrative_features)
    price_features = _normalize_event_time_utc(price_features)

    joined = belief_features.join(
        narrative_features, on=["event_time", "narrative"], how="left"
    ).join(price_features, on=["event_time", "asset"], how="left")

    with_labels = add_directional_labels(joined)
    with_pen = add_penalties(with_labels)
    with_score = add_underreaction_score(with_pen)
    training_examples = add_candidate_flag(with_score)

    live_signals = training_examples.filter(pl.col("is_candidate_event"))

    training_key, live_key = _gold_keys(run_date)

    write_parquet_df(training_examples, training_key)
    write_parquet_df(live_signals, live_key)

    return {
        "training_examples": training_key,
        "live_signals": live_key,
    }
