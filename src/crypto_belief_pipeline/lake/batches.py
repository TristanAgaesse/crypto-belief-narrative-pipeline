from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


def generate_batch_id(now: datetime | None = None, uniqueness: str | None = None) -> str:
    """Generate a time-sortable UTC batch identifier.

    `uniqueness` optionally appends a deterministic suffix to avoid key collisions
    during sub-minute triggering.
    """

    dt = now or datetime.now(UTC)
    dt = dt.astimezone(UTC)
    base = dt.strftime("%Y%m%dT%H%M%SZ")
    if not uniqueness:
        return base
    safe = "".join(ch for ch in uniqueness if ch.isalnum())[:16]
    return f"{base}_{safe}" if safe else base


@dataclass(frozen=True)
class BatchParts:
    date: str  # YYYY-MM-DD
    hour: str  # HH (00-23)
    batch_id: str


def split_batch_parts(batch_id: str) -> BatchParts:
    # Expected: YYYYMMDDTHHMMSSZ
    core = batch_id.split("_", 1)[0]
    dt = datetime.strptime(core, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    return BatchParts(
        date=dt.date().isoformat(),
        hour=f"{dt.hour:02d}",
        batch_id=batch_id,
    )
