from __future__ import annotations

from datetime import UTC

from crypto_belief_pipeline.orchestration._helpers import partitions_def
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
from dagster import RunRequest, ScheduleEvaluationContext, schedule  # type: ignore[attr-defined]


def _daily_partition_key_for_tick(context: ScheduleEvaluationContext) -> str:
    """Map schedule tick time to a daily partition key (UTC, matches lake date=)."""

    dt = context.scheduled_execution_time
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return partitions_def.get_partition_key_for_timestamp(dt.timestamp())


@schedule(
    name="live_market_fast_schedule",
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    job=live_market_fast_job,
)
def live_market_fast_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="live_research_hourly_schedule",
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    job=incremental_live_job,
)
def live_research_hourly_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="binance_raw_1m_schedule",
    cron_schedule="*/1 * * * *",
    execution_timezone="UTC",
    job=binance_raw_1m_job,
)
def binance_raw_1m_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="polymarket_prices_5m_schedule",
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    job=polymarket_prices_5m_job,
)
def polymarket_prices_5m_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="polymarket_discovery_6h_schedule",
    cron_schedule="0 */6 * * *",
    execution_timezone="UTC",
    job=polymarket_discovery_6h_job,
)
def polymarket_discovery_6h_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="gdelt_raw_1h_schedule",
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    job=gdelt_raw_1h_job,
)
def gdelt_raw_1h_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="silver_microbatch_5m_schedule",
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    job=silver_microbatch_5m_job,
)
def silver_microbatch_5m_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="gold_live_signals_5m_schedule",
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    job=gold_live_signals_5m_job,
)
def gold_live_signals_5m_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="label_maturation_1h_schedule",
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    job=label_maturation_1h_job,
)
def label_maturation_1h_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="soda_60m_schedule",
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    job=soda_60m_job,
)
def soda_60m_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="reports_daily_schedule",
    cron_schedule="0 10 * * *",
    execution_timezone="UTC",
    job=reports_daily_job,
)
def reports_daily_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))
