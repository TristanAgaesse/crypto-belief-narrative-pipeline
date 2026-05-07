from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

pytest.importorskip("dagster")

import crypto_belief_pipeline.orchestration._helpers as helpers
from crypto_belief_pipeline.orchestration.assets import _partition_tick_now_utc


def test_partition_tick_now_stays_within_partition_day(monkeypatch) -> None:
    run_date = date(2026, 5, 6)

    # Simulate running far in the future (a backfill scenario).
    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return datetime(2026, 5, 8, 12, 0, 0, tzinfo=UTC)

    # `_partition_tick_now_utc` is defined in `orchestration._helpers`; patch the
    # `datetime` symbol in that module rather than the legacy `assets` re-export.
    monkeypatch.setattr(helpers, "datetime", _FrozenDateTime)

    tick = _partition_tick_now_utc(run_date)

    assert tick.tzinfo == UTC
    assert tick.date().isoformat() == "2026-05-06"
    assert tick.hour == 23
    assert tick.minute == 59
