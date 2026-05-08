from __future__ import annotations

from crypto_belief_pipeline.orchestration.assets import ALL_ASSETS
from crypto_belief_pipeline.orchestration.jobs import ALL_JOBS
from crypto_belief_pipeline.orchestration.schedules import ALL_SCHEDULES
from dagster import Definitions

defs = Definitions(
    assets=ALL_ASSETS,
    jobs=ALL_JOBS,
    schedules=ALL_SCHEDULES,
)
