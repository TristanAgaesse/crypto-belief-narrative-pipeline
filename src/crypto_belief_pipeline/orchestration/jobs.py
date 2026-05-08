from __future__ import annotations

from dagster import AssetSelection, define_asset_job  # type: ignore[attr-defined]

# Source-chained ingestion jobs (raw -> bronze -> silver).
raw_to_silver__binance__1m_job = define_asset_job(
    name="raw_to_silver__binance__1m_job",
    selection=AssetSelection.assets("raw_binance", "bronze_binance", "silver_crypto_candles_1m"),
)

raw_to_silver__polymarket__5m_job = define_asset_job(
    name="raw_to_silver__polymarket__5m_job",
    selection=AssetSelection.assets(
        "raw_polymarket",
        "bronze_polymarket",
        "silver_belief_price_snapshots",
    ),
)

raw_to_silver__polymarket_discovery__6h_job = define_asset_job(
    name="raw_to_silver__polymarket_discovery__6h_job",
    selection=AssetSelection.assets(
        "raw_polymarket",
        "bronze_polymarket",
        "silver_belief_price_snapshots",
    ),
)

raw_to_silver__gdelt__1h_job = define_asset_job(
    name="raw_to_silver__gdelt__1h_job",
    selection=AssetSelection.assets("raw_gdelt", "bronze_gdelt", "silver_narrative_counts"),
)

# Raw-only operational heartbeat for low-latency market collector visibility.
raw__market_fast__5m_job = define_asset_job(
    name="raw__market_fast__5m_job",
    selection=AssetSelection.assets("raw_polymarket", "raw_binance"),
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
    selection=AssetSelection.assets("soda_data_quality", "data_issues"),
)

quality_to_reports__daily_job = define_asset_job(
    name="quality_to_reports__daily_job",
    selection=AssetSelection.assets("markdown_reports"),
)

# Note: bronze→silver assets depend on live `raw_*` collectors, not `raw_sample_inputs`.
# This job is for environments that can hit live APIs; for fully offline sample data use
# `make full-sample` / `pipeline run --mode sample` instead.
full_stack__sample__manual_job = define_asset_job(
    name="full_stack__sample__manual_job",
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

full_stack__live__hourly_job = define_asset_job(
    name="full_stack__live__hourly_job",
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

full_refresh__all_layers__manual_job = define_asset_job(
    name="full_refresh__all_layers__manual_job",
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
