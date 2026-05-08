from __future__ import annotations

from datetime import UTC, datetime, timedelta

from crypto_belief_pipeline.orchestration.windows import compute_window


def test_compute_window_no_cursor_uses_default_start() -> None:
    now = datetime(2026, 5, 7, 12, 34, 56, tzinfo=UTC)
    cadence = timedelta(minutes=5)
    overlap = timedelta(minutes=1)

    w = compute_window(now=now, cadence=cadence, overlap=overlap)

    assert w.end_time == datetime(2026, 5, 7, 12, 34, 56, tzinfo=UTC)
    assert w.start_time == datetime(2026, 5, 7, 12, 28, 56, tzinfo=UTC)
    assert w.start_time < w.end_time


def test_compute_window_uses_default_start_without_cursor_state() -> None:
    now = datetime(2026, 5, 7, 12, 34, 56, tzinfo=UTC)
    cadence = timedelta(minutes=5)
    overlap = timedelta(minutes=1)

    w = compute_window(now=now, cadence=cadence, overlap=overlap)
    assert w.end_time == datetime(2026, 5, 7, 12, 34, 56, tzinfo=UTC)
    assert w.start_time == datetime(2026, 5, 7, 12, 28, 56, tzinfo=UTC)
    assert w.start_time < w.end_time
