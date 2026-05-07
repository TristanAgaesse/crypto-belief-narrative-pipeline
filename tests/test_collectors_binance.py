from __future__ import annotations

from datetime import UTC, datetime

from crypto_belief_pipeline.collectors import binance as bn


def _sample_kline() -> list:
    return [
        1778061600000,  # open_time 2026-05-06T10:00:00Z
        "64000.0",
        "64080.0",
        "63950.0",
        "64050.0",
        "120.5",
        1778061659999,  # close_time 2026-05-06T10:00:59.999Z
        "7710000.0",
        1200,
        "0",
        "0",
        "0",
    ]


def test_to_raw_kline_record_maps_array_correctly() -> None:
    rec = bn.to_raw_kline_record(_sample_kline(), symbol="BTCUSDT", interval="1m")
    assert rec["symbol"] == "BTCUSDT"
    assert rec["interval"] == "1m"
    assert rec["open"] == 64000.0
    assert rec["high"] == 64080.0
    assert rec["low"] == 63950.0
    assert rec["close"] == 64050.0
    assert rec["volume"] == 120.5
    assert rec["quote_volume"] == 7710000.0
    assert rec["number_of_trades"] == 1200
    assert isinstance(rec["raw"], list) and len(rec["raw"]) == 12


def test_to_raw_kline_record_timestamps_are_iso_utc_with_z() -> None:
    rec = bn.to_raw_kline_record(_sample_kline(), symbol="BTCUSDT", interval="1m")
    assert rec["open_time"] == "2026-05-06T10:00:00Z"
    assert rec["close_time"].endswith("Z")
    assert rec["close_time"].startswith("2026-05-06T10:00:")


def test_to_raw_kline_record_rejects_short_array() -> None:
    import pytest

    with pytest.raises(ValueError):
        bn.to_raw_kline_record([1, 2, 3], symbol="BTCUSDT", interval="1m")


def test_collect_binance_raw_iterates_symbols(monkeypatch) -> None:
    calls: list[tuple[str, str, int]] = []

    def fake_fetch(symbol: str, interval: str = "1m", limit: int = 120, **_) -> list[list]:
        calls.append((symbol, interval, limit))
        return [_sample_kline()]

    monkeypatch.setattr(bn, "fetch_klines", fake_fetch)

    out, meta = bn.collect_binance_raw(
        symbols=["BTCUSDT", "ETHUSDT"],
        interval="1m",
        limit=5,
        start_time=datetime(2026, 5, 6, 10, 0, tzinfo=UTC),
        end_time=datetime(2026, 5, 6, 10, 5, tzinfo=UTC),
    )
    assert meta["source"] == "binance"
    assert len(out) == 2
    assert {r["symbol"] for r in out} == {"BTCUSDT", "ETHUSDT"}
    assert calls == [("BTCUSDT", "1m", 5), ("ETHUSDT", "1m", 5)]


def test_fetch_klines_error_json_object_returns_empty(monkeypatch) -> None:
    def fake_get_json(url: str, params: dict | None = None, timeout: float = 20.0):
        return {"code": -1121, "msg": "Invalid symbol."}

    monkeypatch.setattr(bn, "get_json", fake_get_json)
    assert bn.fetch_klines(symbol="BADSYMBOL", interval="1m", limit=5) == []


def test_collect_binance_raw_default_symbols(monkeypatch) -> None:
    seen: list[str] = []

    def fake_fetch(symbol: str, interval: str = "1m", limit: int = 120, **_) -> list[list]:
        seen.append(symbol)
        return [_sample_kline()]

    monkeypatch.setattr(bn, "fetch_klines", fake_fetch)
    bn.collect_binance_raw(
        start_time=datetime(2026, 5, 6, 10, 0, tzinfo=UTC),
        end_time=datetime(2026, 5, 6, 10, 5, tzinfo=UTC),
    )
    assert seen == bn.DEFAULT_SYMBOLS
