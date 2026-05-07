from __future__ import annotations

from dagster import Definitions

from crypto_belief_pipeline.orchestration.assets import ALL_ASSETS
from crypto_belief_pipeline.orchestration.jobs import (
    full_refresh_job,
    incremental_live_job,
    incremental_sample_job,
    live_market_fast_job,
)
from crypto_belief_pipeline.orchestration.schedules import (
    live_market_fast_schedule,
    live_research_hourly_schedule,
    sample_daily_schedule,
)

defs = Definitions(
    assets=ALL_ASSETS,
    jobs=[incremental_sample_job, incremental_live_job, live_market_fast_job, full_refresh_job],
    schedules=[sample_daily_schedule, live_market_fast_schedule, live_research_hourly_schedule],
)
