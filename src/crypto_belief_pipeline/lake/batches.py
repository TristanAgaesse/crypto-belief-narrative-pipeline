from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def generate_batch_id(now: datetime | None = None) -> str:
    """Generate a time-sortable UTC batch identifier."""

    dt = now or datetime.now(timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y%m%dT%H%M%SZ")


@dataclass(frozen=True)
class BatchParts:
    date: str  # YYYY-MM-DD
    hour: str  # HH (00-23)
    batch_id: str


def split_batch_parts(batch_id: str) -> BatchParts:
    # Expected: YYYYMMDDTHHMMSSZ
    dt = datetime.strptime(batch_id, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return BatchParts(
        date=dt.date().isoformat(),
        hour=f"{dt.hour:02d}",
        batch_id=batch_id,
    )

