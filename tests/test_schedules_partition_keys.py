from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("dagster")

from crypto_belief_pipeline.orchestration._helpers import raw_bronze_minute_partitions_def
from crypto_belief_pipeline.orchestration.schedules import _minute_partition_key_for_tick


class _SchedCtx:
    __slots__ = ("scheduled_execution_time",)

    def __init__(self, scheduled_execution_time: datetime) -> None:
        self.scheduled_execution_time = scheduled_execution_time


def test_minute_partition_key_on_boundary_uses_previous_minute() -> None:
    """Cron at HH:MM:00 must not map to the in-progress minute window."""
    dt = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
    pk = _minute_partition_key_for_tick(_SchedCtx(dt))
    assert pk == "2026-05-07T11:59"


def test_minute_partition_key_matches_anchor_timestamp() -> None:
    dt = datetime(2026, 5, 7, 12, 34, 56, tzinfo=UTC)
    pk = _minute_partition_key_for_tick(_SchedCtx(dt))
    anchor = dt - timedelta(seconds=1)
    expected = raw_bronze_minute_partitions_def.get_partition_key_for_timestamp(anchor.timestamp())
    assert pk == expected
    assert pk == "2026-05-07T12:33"
