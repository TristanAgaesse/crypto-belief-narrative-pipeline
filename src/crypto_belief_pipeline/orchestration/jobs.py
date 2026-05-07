from __future__ import annotations

from dagster import AssetSelection, define_asset_job

incremental_sample_job = define_asset_job(
    name="incremental_sample_job",
    selection=AssetSelection.assets(
        "raw_sample_inputs",
        "bronze_polymarket",
        "bronze_binance",
        "bronze_gdelt",
        "silver_belief_price_snapshots",
        "silver_crypto_candles_1m",
        "silver_narrative_counts",
        "gold_training_examples",
        "gold_alpha_events",
        "soda_data_quality",
        "data_issues",
        "markdown_reports",
    ),
)

incremental_live_job = define_asset_job(
    name="incremental_live_job",
    selection=AssetSelection.assets(
        "raw_polymarket",
        "raw_binance",
        "raw_gdelt",
        "bronze_polymarket",
        "bronze_binance",
        "bronze_gdelt",
        "silver_belief_price_snapshots",
        "silver_crypto_candles_1m",
        "silver_narrative_counts",
        "gold_training_examples",
        "gold_alpha_events",
        "soda_data_quality",
        "data_issues",
        "markdown_reports",
    ),
)

live_market_fast_job = define_asset_job(
    name="live_market_fast_job",
    selection=AssetSelection.assets("raw_polymarket", "raw_binance"),
)

full_refresh_job = define_asset_job(
    name="full_refresh_job",
    selection=AssetSelection.assets(
        "raw_sample_inputs",
        "raw_polymarket",
        "raw_binance",
        "raw_gdelt",
        "bronze_polymarket",
        "bronze_binance",
        "bronze_gdelt",
        "silver_belief_price_snapshots",
        "silver_crypto_candles_1m",
        "silver_narrative_counts",
        "gold_training_examples",
        "gold_alpha_events",
        "soda_data_quality",
        "data_issues",
        "markdown_reports",
    ),
)
