from __future__ import annotations

from crypto_belief_pipeline.orchestration.assets import ALL_ASSETS
from crypto_belief_pipeline.orchestration.jobs import (
    full_refresh__all_layers__manual_job,
    full_stack__live__hourly_job,
    full_stack__sample__manual_job,
    gold__label_maturation__1h_job,
    gold_to_quality__hourly_job,
    quality_to_reports__daily_job,
    raw__market_fast__5m_job,
    raw_to_silver__binance__1m_job,
    raw_to_silver__gdelt__1h_job,
    raw_to_silver__polymarket__5m_job,
    raw_to_silver__polymarket_discovery__6h_job,
    silver_to_gold__signals__5m_job,
)
from crypto_belief_pipeline.orchestration.schedules import (
    full_stack__live__hourly_schedule,
    gold__label_maturation__1h_schedule,
    gold_to_quality__hourly_schedule,
    quality_to_reports__daily_schedule,
    raw__market_fast__5m_schedule,
    raw_to_silver__binance__1m_schedule,
    raw_to_silver__gdelt__1h_schedule,
    raw_to_silver__polymarket__5m_schedule,
    raw_to_silver__polymarket_discovery__6h_schedule,
    silver_to_gold__signals__5m_schedule,
)
from dagster import Definitions

defs = Definitions(
    assets=ALL_ASSETS,
    jobs=[
        full_stack__sample__manual_job,
        full_stack__live__hourly_job,
        raw__market_fast__5m_job,
        full_refresh__all_layers__manual_job,
        raw_to_silver__binance__1m_job,
        raw_to_silver__polymarket__5m_job,
        raw_to_silver__polymarket_discovery__6h_job,
        raw_to_silver__gdelt__1h_job,
        silver_to_gold__signals__5m_job,
        gold__label_maturation__1h_job,
        gold_to_quality__hourly_job,
        quality_to_reports__daily_job,
    ],
    schedules=[
        raw__market_fast__5m_schedule,
        full_stack__live__hourly_schedule,
        raw_to_silver__binance__1m_schedule,
        raw_to_silver__polymarket__5m_schedule,
        raw_to_silver__polymarket_discovery__6h_schedule,
        raw_to_silver__gdelt__1h_schedule,
        silver_to_gold__signals__5m_schedule,
        gold__label_maturation__1h_schedule,
        gold_to_quality__hourly_schedule,
        quality_to_reports__daily_schedule,
    ],
)
