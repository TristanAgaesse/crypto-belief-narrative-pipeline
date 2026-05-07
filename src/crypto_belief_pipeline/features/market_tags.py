from __future__ import annotations

from pathlib import Path

import polars as pl

REQUIRED_COLUMNS: tuple[str, ...] = (
    "market_id",
    "asset",
    "narrative",
    "direction",
    "relevance",
    "confidence",
    "notes",
)

ALLOWED_ASSETS: frozenset[str] = frozenset({"BTC", "ETH", "SOL"})
ALLOWED_DIRECTIONS: frozenset[int] = frozenset({-1, 1})
ALLOWED_RELEVANCE: frozenset[str] = frozenset({"low", "medium", "high"})

DEFAULT_MARKET_TAGS_PATH = "data/sample/market_tags.csv"


def load_market_tags(path: str | Path = DEFAULT_MARKET_TAGS_PATH) -> pl.DataFrame:
    """Load manually curated market tags from CSV.

    The CSV must contain the columns listed in ``REQUIRED_COLUMNS``. We do not
    validate here; callers should run :func:`validate_market_tags` before use.
    """

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Market tags file not found: {p}")
    return pl.read_csv(p)


def validate_market_tags(tags: pl.DataFrame) -> pl.DataFrame:
    """Validate and normalize market tags.

    Enforces:
    - required columns present
    - direction in {-1, 1}
    - asset in {BTC, ETH, SOL}
    - confidence in [0, 1]
    - relevance in {low, medium, high}
    - no duplicate (market_id, asset, narrative)

    Returns a frame with normalized dtypes.
    """

    missing = [c for c in REQUIRED_COLUMNS if c not in tags.columns]
    if missing:
        raise ValueError(f"market_tags missing required columns: {missing}")

    normalized = tags.select(
        pl.col("market_id").cast(pl.String),
        pl.col("asset").cast(pl.String),
        pl.col("narrative").cast(pl.String),
        pl.col("direction").cast(pl.Int64),
        pl.col("relevance").cast(pl.String),
        pl.col("confidence").cast(pl.Float64),
        pl.col("notes").cast(pl.String),
    )

    bad_direction = normalized.filter(~pl.col("direction").is_in(list(ALLOWED_DIRECTIONS)))
    if bad_direction.height > 0:
        raise ValueError(
            f"market_tags has invalid direction values: {bad_direction['direction'].to_list()}"
        )

    bad_asset = normalized.filter(~pl.col("asset").is_in(list(ALLOWED_ASSETS)))
    if bad_asset.height > 0:
        raise ValueError(f"market_tags has invalid asset values: {bad_asset['asset'].to_list()}")

    bad_relevance = normalized.filter(~pl.col("relevance").is_in(list(ALLOWED_RELEVANCE)))
    if bad_relevance.height > 0:
        raise ValueError(
            f"market_tags has invalid relevance values: {bad_relevance['relevance'].to_list()}"
        )

    bad_confidence = normalized.filter(
        pl.col("confidence").is_null() | (pl.col("confidence") < 0.0) | (pl.col("confidence") > 1.0)
    )
    if bad_confidence.height > 0:
        raise ValueError(
            f"market_tags has invalid confidence values: {bad_confidence['confidence'].to_list()}"
        )

    dup_count = (
        normalized.group_by(["market_id", "asset", "narrative"])
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") > 1)
    )
    if dup_count.height > 0:
        raise ValueError(
            "market_tags has duplicate (market_id, asset, narrative) rows: "
            f"{dup_count.select(['market_id', 'asset', 'narrative']).to_dicts()}"
        )

    return normalized
