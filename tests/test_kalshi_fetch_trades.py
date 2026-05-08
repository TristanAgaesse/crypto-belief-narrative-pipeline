"""Regression tests for Kalshi trades pagination (must not hang)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from crypto_belief_pipeline.collectors import kalshi as k


def test_fetch_trades_stops_after_max_pages_when_cursor_never_clears(monkeypatch) -> None:
    """Empty pages with a perpetual cursor used to spin forever (no row growth)."""

    calls: list[int] = []

    def fake_get_json(url: str, params: dict | None = None):
        calls.append(1)
        return {"trades": [], "cursor": "stuck"}

    monkeypatch.setattr(k, "get_json", fake_get_json)
    cfg = {"trades_max_rows": 8000, "trades_max_pages": 5}
    rows, err = k.fetch_trades(cfg)
    assert rows == []
    assert err is None
    assert len(calls) == 5


def test_fetch_trades_respects_max_pages_even_when_rows_not_full(monkeypatch) -> None:
    """Cap pages even if we never reach trades_max_rows."""

    def fake_get_json(url: str, params: dict | None = None):
        return {"trades": [{"trade_id": "a"}], "cursor": "more"}

    monkeypatch.setattr(k, "get_json", fake_get_json)
    cfg = {"trades_max_rows": 10_000, "trades_max_pages": 3}
    rows, err = k.fetch_trades(cfg)
    assert len(rows) == 3
    assert err is None
