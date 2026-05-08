from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True)
class Window:
    start_time: datetime
    end_time: datetime


def _normalize_utc_second(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.replace(microsecond=0)


def compute_window(
    *,
    now: datetime,
    cadence: timedelta,
    overlap: timedelta,
) -> Window:
    """Compute `[start, end)` window in UTC.

    Policy:
    - `end` is normalized to UTC with second precision.
    - `start` is `end - cadence - overlap`.
    """

    end_time = _normalize_utc_second(now)
    start_time = end_time - cadence - overlap

    if start_time >= end_time:
        start_time = end_time - cadence

    return Window(start_time=start_time, end_time=end_time)
