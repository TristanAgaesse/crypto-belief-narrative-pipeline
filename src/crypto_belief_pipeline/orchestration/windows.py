from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from crypto_belief_pipeline.state.ingestion_cursors import IngestionCursor


@dataclass(frozen=True)
class Window:
    start_time: datetime
    end_time: datetime


def _floor_to_minute(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.replace(second=0, microsecond=0)


def compute_window(
    *,
    now: datetime,
    cadence: timedelta,
    overlap: timedelta,
    cursor: IngestionCursor | None,
) -> Window:
    """Compute `[start, end)` window in UTC.

    Policy:
    - `end` is floored to minute for stability.
    - `start` is max(cursor_time - overlap, end - cadence - overlap) when cursor exists.
    - if no cursor, start = end - cadence - overlap.
    """

    end_time = _floor_to_minute(now)

    default_start = end_time - cadence - overlap
    if cursor and cursor.last_successful_event_time:
        try:
            last = datetime.fromisoformat(cursor.last_successful_event_time.replace("Z", "+00:00"))
            last = _floor_to_minute(last)
            start_time = max(default_start, last - overlap)
        except Exception:
            start_time = default_start
    else:
        start_time = default_start

    if start_time >= end_time:
        start_time = end_time - cadence

    return Window(start_time=start_time, end_time=end_time)
