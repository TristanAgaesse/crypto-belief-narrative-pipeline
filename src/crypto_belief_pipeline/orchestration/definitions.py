from __future__ import annotations

from crypto_belief_pipeline.orchestration.assets import ALL_ASSETS
from crypto_belief_pipeline.orchestration.jobs import (
    binance_raw_1m_job,
    full_refresh_job,
    gdelt_raw_1h_job,
    gold_live_signals_5m_job,
    incremental_live_job,
    incremental_sample_job,
    label_maturation_1h_job,
    live_market_fast_job,
    polymarket_discovery_6h_job,
    polymarket_prices_5m_job,
    reports_daily_job,
    silver_microbatch_5m_job,
    soda_60m_job,
)
from crypto_belief_pipeline.orchestration.schedules import (
    binance_raw_1m_schedule,
    gdelt_raw_1h_schedule,
    gold_live_signals_5m_schedule,
    label_maturation_1h_schedule,
    live_market_fast_schedule,
    live_research_hourly_schedule,
    polymarket_discovery_6h_schedule,
    polymarket_prices_5m_schedule,
    reports_daily_schedule,
    silver_microbatch_5m_schedule,
    soda_60m_schedule,
)
from dagster import Definitions

defs = Definitions(
    assets=ALL_ASSETS,
    jobs=[
        incremental_sample_job,
        incremental_live_job,
        live_market_fast_job,
        full_refresh_job,
        binance_raw_1m_job,
        polymarket_prices_5m_job,
        polymarket_discovery_6h_job,
        gdelt_raw_1h_job,
        silver_microbatch_5m_job,
        gold_live_signals_5m_job,
        label_maturation_1h_job,
        soda_60m_job,
        reports_daily_job,
    ],
    schedules=[
        live_market_fast_schedule,
        live_research_hourly_schedule,
        binance_raw_1m_schedule,
        polymarket_prices_5m_schedule,
        polymarket_discovery_6h_schedule,
        gdelt_raw_1h_schedule,
        silver_microbatch_5m_schedule,
        gold_live_signals_5m_schedule,
        label_maturation_1h_schedule,
        soda_60m_schedule,
        reports_daily_schedule,
    ],
)
