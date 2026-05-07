from __future__ import annotations

from dagster import AssetSelection, define_asset_job

binance_raw_1m_job = define_asset_job(
    name="binance_raw_1m_job",
    selection=AssetSelection.assets("raw_binance"),
)

polymarket_prices_5m_job = define_asset_job(
    name="polymarket_prices_5m_job",
    selection=AssetSelection.assets("raw_polymarket"),
)

polymarket_discovery_6h_job = define_asset_job(
    name="polymarket_discovery_6h_job",
    selection=AssetSelection.assets("raw_polymarket"),
)

gdelt_raw_1h_job = define_asset_job(
    name="gdelt_raw_1h_job",
    selection=AssetSelection.assets("raw_gdelt"),
)

silver_microbatch_5m_job = define_asset_job(
    name="silver_microbatch_5m_job",
    selection=AssetSelection.assets(
        "bronze_polymarket",
        "bronze_binance",
        "bronze_gdelt",
        "silver_belief_price_snapshots",
        "silver_crypto_candles_1m",
        "silver_narrative_counts",
    ),
)

gold_live_signals_5m_job = define_asset_job(
    name="gold_live_signals_5m_job",
    selection=AssetSelection.assets("gold_training_examples", "gold_live_signals"),
)

label_maturation_1h_job = define_asset_job(
    name="label_maturation_1h_job",
    selection=AssetSelection.assets("gold_training_examples"),
)

soda_60m_job = define_asset_job(
    name="soda_60m_job",
    selection=AssetSelection.assets("soda_data_quality"),
)

reports_daily_job = define_asset_job(
    name="reports_daily_job",
    selection=AssetSelection.assets("markdown_reports"),
)

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
        "gold_live_signals",
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
        "gold_live_signals",
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
        "gold_live_signals",
        "soda_data_quality",
        "data_issues",
        "markdown_reports",
    ),
)
