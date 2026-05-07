from __future__ import annotations

from crypto_belief_pipeline.orchestration.jobs import (
    binance_raw_1m_job,
    gdelt_raw_1h_job,
    gold_live_signals_5m_job,
    incremental_live_job,
    label_maturation_1h_job,
    live_market_fast_job,
    polymarket_discovery_6h_job,
    polymarket_prices_5m_job,
    reports_daily_job,
    silver_microbatch_5m_job,
    soda_60m_job,
)
from dagster import ScheduleDefinition  # type: ignore[attr-defined]

live_market_fast_schedule = ScheduleDefinition(
    name="live_market_fast_schedule",
    cron_schedule="*/5 * * * *",
    job=live_market_fast_job,
)


live_research_hourly_schedule = ScheduleDefinition(
    name="live_research_hourly_schedule",
    cron_schedule="0 * * * *",
    job=incremental_live_job,
)


binance_raw_1m_schedule = ScheduleDefinition(
    name="binance_raw_1m_schedule",
    cron_schedule="*/1 * * * *",
    job=binance_raw_1m_job,
)

polymarket_prices_5m_schedule = ScheduleDefinition(
    name="polymarket_prices_5m_schedule",
    cron_schedule="*/5 * * * *",
    job=polymarket_prices_5m_job,
)

polymarket_discovery_6h_schedule = ScheduleDefinition(
    name="polymarket_discovery_6h_schedule",
    cron_schedule="0 */6 * * *",
    job=polymarket_discovery_6h_job,
)

gdelt_raw_1h_schedule = ScheduleDefinition(
    name="gdelt_raw_1h_schedule",
    cron_schedule="0 * * * *",
    job=gdelt_raw_1h_job,
)

silver_microbatch_5m_schedule = ScheduleDefinition(
    name="silver_microbatch_5m_schedule",
    cron_schedule="*/5 * * * *",
    job=silver_microbatch_5m_job,
)

gold_live_signals_5m_schedule = ScheduleDefinition(
    name="gold_live_signals_5m_schedule",
    cron_schedule="*/5 * * * *",
    job=gold_live_signals_5m_job,
)

label_maturation_1h_schedule = ScheduleDefinition(
    name="label_maturation_1h_schedule",
    cron_schedule="0 * * * *",
    job=label_maturation_1h_job,
)

soda_60m_schedule = ScheduleDefinition(
    name="soda_60m_schedule",
    cron_schedule="0 * * * *",
    job=soda_60m_job,
)

reports_daily_schedule = ScheduleDefinition(
    name="reports_daily_schedule",
    cron_schedule="0 10 * * *",
    job=reports_daily_job,
)
