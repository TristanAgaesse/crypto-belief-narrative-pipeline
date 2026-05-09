from __future__ import annotations

from dagster import AssetSelection, define_asset_job  # type: ignore[attr-defined]

# Minute staging jobs (append-only ingest).
raw_staging__binance__1m_job = define_asset_job(
    name="raw_staging__binance__1m_job",
    selection=AssetSelection.assets("raw_binance_staging"),
)

raw_staging__polymarket__5m_job = define_asset_job(
    name="raw_staging__polymarket__5m_job",
    selection=AssetSelection.assets("raw_polymarket_staging"),
)

raw_staging__polymarket_discovery__6h_job = define_asset_job(
    name="raw_staging__polymarket_discovery__6h_job",
    selection=AssetSelection.assets("raw_polymarket_staging"),
)

raw_staging__gdelt__1h_job = define_asset_job(
    name="raw_staging__gdelt__1h_job",
    selection=AssetSelection.assets("raw_gdelt_staging"),
)

raw_staging__kalshi__5m_job = define_asset_job(
    name="raw_staging__kalshi__5m_job",
    selection=AssetSelection.assets("raw_kalshi_staging"),
)

raw_staging__fear_greed__1h_job = define_asset_job(
    name="raw_staging__fear_greed__1h_job",
    selection=AssetSelection.assets("raw_fear_greed_staging"),
)

# Canonical hourly chains (recomputable overwrite by partition).
raw_to_silver__binance__1m_job = define_asset_job(
    name="raw_to_silver__binance__1m_job",
    selection=AssetSelection.assets("raw_binance", "bronze_binance", "silver_crypto_candles_1m"),
)

raw_to_silver__polymarket__5m_job = define_asset_job(
    name="raw_to_silver__polymarket__5m_job",
    selection=AssetSelection.assets(
        "raw_polymarket", "bronze_polymarket", "silver_belief_price_snapshots"
    ),
)

raw_to_silver__polymarket_discovery__6h_job = define_asset_job(
    name="raw_to_silver__polymarket_discovery__6h_job",
    selection=AssetSelection.assets(
        "raw_polymarket", "bronze_polymarket", "silver_belief_price_snapshots"
    ),
)

raw_to_silver__gdelt__1h_job = define_asset_job(
    name="raw_to_silver__gdelt__1h_job",
    selection=AssetSelection.assets("raw_gdelt", "bronze_gdelt", "silver_narrative_counts"),
)

raw_to_silver__kalshi__5m_job = define_asset_job(
    name="raw_to_silver__kalshi__5m_job",
    selection=AssetSelection.assets(
        "raw_kalshi",
        "bronze_kalshi",
        "silver_kalshi_markets",
        "silver_kalshi_market_snapshots",
        "silver_kalshi_events",
        "silver_kalshi_series",
        "silver_kalshi_trades",
        "silver_kalshi_orderbook_snapshots",
        "silver_kalshi_candlesticks",
        "silver_kalshi_event_repricing_features",
    ),
)

raw_to_silver__fear_greed__1h_job = define_asset_job(
    name="raw_to_silver__fear_greed__1h_job",
    selection=AssetSelection.assets(
        "raw_fear_greed",
        "bronze_fear_greed",
        "silver_fear_greed_daily",
        "silver_fear_greed_regime_features",
    ),
)

silver_to_gold__signals__5m_job = define_asset_job(
    name="silver_to_gold__signals__5m_job",
    selection=AssetSelection.assets("gold_training_examples", "gold_live_signals"),
)

gold__label_maturation__1h_job = define_asset_job(
    name="gold__label_maturation__1h_job",
    # `gold_training_examples` is one output of a non-subsettable multi-asset; include neighbors.
    selection=AssetSelection.assets("gold_training_examples").required_multi_asset_neighbors(),
)

gold_to_quality__hourly_job = define_asset_job(
    name="gold_to_quality__hourly_job",
    selection=AssetSelection.assets("soda_data_quality", "processing_gaps", "data_issues"),
)

quality_to_reports__daily_job = define_asset_job(
    name="quality_to_reports__daily_job",
    selection=AssetSelection.assets("markdown_reports"),
)

#
# IMPORTANT:
# Dagster asset jobs require all selected assets to share a partitions definition.
# The pipeline uses different partitions by tier (minute for raw staging, hourly for
# canonical raw->bronze->silver->gold->quality->reports). So we replace the previous
# mixed-partition full-stack jobs with partition-compatible manual entrypoints.
#

# Manual: minute staging only.
full_stack__minute__manual_job = define_asset_job(
    name="full_stack__minute__manual_job",
    selection=AssetSelection.assets(
        "raw_polymarket_staging",
        "raw_binance_staging",
        "raw_gdelt_staging",
        "raw_kalshi_staging",
    ),
)

# Manual: canonical hourly full stack (single source of truth for tests/docs alignment).
FULL_STACK_HOURLY_ASSET_NAMES: tuple[str, ...] = (
    "raw_polymarket",
    "raw_binance",
    "raw_gdelt",
    "raw_kalshi",
    "raw_fear_greed",
    "bronze_polymarket",
    "bronze_binance",
    "bronze_gdelt",
    "bronze_kalshi",
    "bronze_fear_greed",
    "silver_belief_price_snapshots",
    "silver_crypto_candles_1m",
    "silver_narrative_counts",
    "silver_fear_greed_daily",
    "silver_fear_greed_regime_features",
    "silver_kalshi_markets",
    "silver_kalshi_market_snapshots",
    "silver_kalshi_events",
    "silver_kalshi_series",
    "silver_kalshi_trades",
    "silver_kalshi_orderbook_snapshots",
    "silver_kalshi_candlesticks",
    "silver_kalshi_event_repricing_features",
    "gold_training_examples",
    "gold_live_signals",
    "soda_data_quality",
    "processing_gaps",
    "data_issues",
    "markdown_reports",
)

full_stack__hourly__manual_job = define_asset_job(
    name="full_stack__hourly__manual_job",
    selection=AssetSelection.assets(*FULL_STACK_HOURLY_ASSET_NAMES),
)


ALL_JOBS = [
    raw_staging__binance__1m_job,
    raw_staging__polymarket__5m_job,
    raw_staging__polymarket_discovery__6h_job,
    raw_staging__gdelt__1h_job,
    raw_staging__kalshi__5m_job,
    raw_staging__fear_greed__1h_job,
    raw_to_silver__binance__1m_job,
    raw_to_silver__polymarket__5m_job,
    raw_to_silver__polymarket_discovery__6h_job,
    raw_to_silver__gdelt__1h_job,
    raw_to_silver__kalshi__5m_job,
    raw_to_silver__fear_greed__1h_job,
    silver_to_gold__signals__5m_job,
    gold__label_maturation__1h_job,
    gold_to_quality__hourly_job,
    quality_to_reports__daily_job,
    full_stack__minute__manual_job,
    full_stack__hourly__manual_job,
]


__all__ = [
    "ALL_JOBS",
    "FULL_STACK_HOURLY_ASSET_NAMES",
    "full_stack__hourly__manual_job",
    "full_stack__minute__manual_job",
    "gold__label_maturation__1h_job",
    "gold_to_quality__hourly_job",
    "quality_to_reports__daily_job",
    "raw_staging__binance__1m_job",
    "raw_staging__fear_greed__1h_job",
    "raw_staging__gdelt__1h_job",
    "raw_staging__kalshi__5m_job",
    "raw_staging__polymarket__5m_job",
    "raw_staging__polymarket_discovery__6h_job",
    "raw_to_silver__binance__1m_job",
    "raw_to_silver__fear_greed__1h_job",
    "raw_to_silver__gdelt__1h_job",
    "raw_to_silver__kalshi__5m_job",
    "raw_to_silver__polymarket__5m_job",
    "raw_to_silver__polymarket_discovery__6h_job",
    "silver_to_gold__signals__5m_job",
]
