from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from crypto_belief_pipeline.orchestration.resources import (
    resolve_partition_window_from_context,
    resolve_run_date,
)


def test_resolve_run_date_daily_key() -> None:
    assert resolve_run_date("2026-05-06").isoformat() == "2026-05-06"


def test_resolve_run_date_minute_key() -> None:
    assert resolve_run_date("2026-05-06T12:34").isoformat() == "2026-05-06"


def test_resolve_run_date_minute_key_legacy_tz_format() -> None:
    assert resolve_run_date("2026-05-06T12:34+0000").isoformat() == "2026-05-06"


def test_resolve_run_date_hourly_key() -> None:
    assert resolve_run_date("2026-05-06-12:00").isoformat() == "2026-05-06"


def test_resolve_partition_window_from_context_uses_partition_time_window_and_utc() -> None:
    # tz-naive window should be treated as UTC
    st = datetime(2026, 5, 6, 12, 0, 0)
    et = datetime(2026, 5, 6, 13, 0, 0)
    ctx = SimpleNamespace(
        has_partition_key=True,
        partition_key="2026-05-06-12:00",
        partition_time_window=SimpleNamespace(start=st, end=et),
    )
    out = resolve_partition_window_from_context(ctx)
    assert out is not None
    assert out.partition_key == "2026-05-06-12:00"
    assert out.start.tzinfo == UTC
    assert out.end.tzinfo == UTC
    assert out.start.hour == 12
    assert out.end.hour == 13


def test_resolve_partition_window_from_context_returns_none_when_unpartitioned() -> None:
    ctx = SimpleNamespace(has_partition_key=False)
    assert resolve_partition_window_from_context(ctx) is None


def test_resolve_run_date_none_partition_key_is_today_utc(monkeypatch) -> None:
    # Freeze "now" by monkeypatching datetime.now in the module under test.
    from crypto_belief_pipeline.orchestration import resources as res

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return datetime(2026, 5, 7, 23, 59, 0, tzinfo=UTC)

    monkeypatch.setattr(res, "datetime", _FrozenDateTime)
    assert res.resolve_run_date(None).isoformat() == "2026-05-07"
