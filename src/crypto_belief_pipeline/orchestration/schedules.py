from __future__ import annotations

from datetime import UTC

from crypto_belief_pipeline.orchestration._helpers import partitions_def
from crypto_belief_pipeline.orchestration.jobs import (
    full_stack__live__hourly_job,
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
from dagster import RunRequest, ScheduleEvaluationContext, schedule  # type: ignore[attr-defined]


def _daily_partition_key_for_tick(context: ScheduleEvaluationContext) -> str:
    """Map schedule tick time to a daily partition key (UTC, matches lake date=)."""

    dt = context.scheduled_execution_time
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return partitions_def.get_partition_key_for_timestamp(dt.timestamp())


@schedule(
    name="raw__market_fast__5m_schedule",
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    job=raw__market_fast__5m_job,
)
def raw__market_fast__5m_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="full_stack__live__hourly_schedule",
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    job=full_stack__live__hourly_job,
)
def full_stack__live__hourly_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="raw_to_silver__binance__1m_schedule",
    cron_schedule="*/1 * * * *",
    execution_timezone="UTC",
    job=raw_to_silver__binance__1m_job,
)
def raw_to_silver__binance__1m_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="raw_to_silver__polymarket__5m_schedule",
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    job=raw_to_silver__polymarket__5m_job,
)
def raw_to_silver__polymarket__5m_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="raw_to_silver__polymarket_discovery__6h_schedule",
    cron_schedule="0 */6 * * *",
    execution_timezone="UTC",
    job=raw_to_silver__polymarket_discovery__6h_job,
)
def raw_to_silver__polymarket_discovery__6h_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="raw_to_silver__gdelt__1h_schedule",
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    job=raw_to_silver__gdelt__1h_job,
)
def raw_to_silver__gdelt__1h_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="silver_to_gold__signals__5m_schedule",
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    job=silver_to_gold__signals__5m_job,
)
def silver_to_gold__signals__5m_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="gold__label_maturation__1h_schedule",
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    job=gold__label_maturation__1h_job,
)
def gold__label_maturation__1h_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="gold_to_quality__hourly_schedule",
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    job=gold_to_quality__hourly_job,
)
def gold_to_quality__hourly_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


@schedule(
    name="quality_to_reports__daily_schedule",
    cron_schedule="0 10 * * *",
    execution_timezone="UTC",
    job=quality_to_reports__daily_job,
)
def quality_to_reports__daily_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(partition_key=_daily_partition_key_for_tick(context))


ALL_SCHEDULES = [
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
]


__all__ = [
    "ALL_SCHEDULES",
    "full_stack__live__hourly_schedule",
    "gold__label_maturation__1h_schedule",
    "gold_to_quality__hourly_schedule",
    "quality_to_reports__daily_schedule",
    "raw__market_fast__5m_schedule",
    "raw_to_silver__binance__1m_schedule",
    "raw_to_silver__gdelt__1h_schedule",
    "raw_to_silver__polymarket__5m_schedule",
    "raw_to_silver__polymarket_discovery__6h_schedule",
    "silver_to_gold__signals__5m_schedule",
]
