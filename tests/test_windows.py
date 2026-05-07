from __future__ import annotations

from datetime import UTC, datetime, timedelta

from crypto_belief_pipeline.orchestration.windows import compute_window
from crypto_belief_pipeline.state.ingestion_cursors import IngestionCursor


def test_compute_window_no_cursor_uses_default_start() -> None:
    now = datetime(2026, 5, 7, 12, 34, 56, tzinfo=UTC)
    cadence = timedelta(minutes=5)
    overlap = timedelta(minutes=1)

    w = compute_window(now=now, cadence=cadence, overlap=overlap, cursor=None)

    assert w.end_time == datetime(2026, 5, 7, 12, 34, 0, tzinfo=UTC)
    assert w.start_time == datetime(2026, 5, 7, 12, 28, 0, tzinfo=UTC)
    assert w.start_time < w.end_time


def test_compute_window_with_cursor_uses_max_of_default_and_cursor_minus_overlap() -> None:
    # end floored to minute => 12:34:00
    now = datetime(2026, 5, 7, 12, 34, 59, tzinfo=UTC)
    cadence = timedelta(minutes=5)
    overlap = timedelta(minutes=1)

    cursor = IngestionCursor(
        source="binance",
        key="symbols=BTCUSDT",
        # Cursor says we last successfully ingested event-time at 12:33:10Z
        # floored => 12:33:00Z, minus overlap => 12:32:00Z
        last_successful_event_time="2026-05-07T12:33:10Z",
        status="ok",
    )

    w = compute_window(now=now, cadence=cadence, overlap=overlap, cursor=cursor)

    # default_start = 12:34 - 5m - 1m = 12:28
    # cursor_start = 12:32
    # start should be max(default_start, cursor_start) = 12:32
    assert w.end_time == datetime(2026, 5, 7, 12, 34, 0, tzinfo=UTC)
    assert w.start_time == datetime(2026, 5, 7, 12, 32, 0, tzinfo=UTC)
    assert w.start_time < w.end_time


def test_compute_window_bad_cursor_falls_back_to_default_start() -> None:
    now = datetime(2026, 5, 7, 12, 34, 56, tzinfo=UTC)
    cadence = timedelta(minutes=5)
    overlap = timedelta(minutes=1)

    cursor = IngestionCursor(
        source="binance",
        key="symbols=BTCUSDT",
        last_successful_event_time="not-a-timestamp",
        status="ok",
    )

    w = compute_window(now=now, cadence=cadence, overlap=overlap, cursor=cursor)
    assert w.end_time == datetime(2026, 5, 7, 12, 34, 0, tzinfo=UTC)
    assert w.start_time == datetime(2026, 5, 7, 12, 28, 0, tzinfo=UTC)
    assert w.start_time < w.end_time

