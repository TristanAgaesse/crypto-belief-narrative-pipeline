from __future__ import annotations

from dagster import ScheduleDefinition

from crypto_belief_pipeline.orchestration.jobs import (
    incremental_live_job,
    incremental_sample_job,
    live_market_fast_job,
)

sample_daily_schedule = ScheduleDefinition(
    name="sample_daily_schedule",
    cron_schedule="0 9 * * *",
    job=incremental_sample_job,
)


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
