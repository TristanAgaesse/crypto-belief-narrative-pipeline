from __future__ import annotations

from datetime import datetime

import crypto_belief_pipeline.collectors.run_live_collectors as rlc


def test_run_live_collectors_writes_expected_keys(monkeypatch) -> None:
    written: list[tuple[str, int]] = []

    def fake_write_jsonl_records(records, key, bucket=None):  # noqa: ANN001
        written.append((key, len(records)))

    def fake_polymarket(*, start_time, end_time, **_kw):  # noqa: ANN001
        assert isinstance(start_time, datetime) and isinstance(end_time, datetime)
        return (
            [{"market_id": "m1", "raw": {"updatedAt": "2026-05-06T00:00:00Z"}}],
            [{"market_id": "m1", "outcome": "Yes"}],
            {"source": "polymarket", "start_time": "x", "end_time": "y"},
        )

    def fake_binance(*, start_time, end_time, **_kw):  # noqa: ANN001
        assert isinstance(start_time, datetime) and isinstance(end_time, datetime)
        return ([{"symbol": "BTCUSDT", "open_time": "2026-05-06T00:00:00Z"}], {"source": "binance"})

    def fake_gdelt(  # noqa: ANN001
        start_date, end_date, narratives_config_path="config/narratives.yaml", **kwargs
    ):
        return [{"narrative": "foo", "timestamp": "2026-05-05T12:00:00Z"}]

    monkeypatch.setattr(rlc, "write_jsonl_records", fake_write_jsonl_records)
    monkeypatch.setattr(rlc, "collect_polymarket_raw", fake_polymarket)
    monkeypatch.setattr(rlc, "collect_binance_raw", fake_binance)
    monkeypatch.setattr(rlc, "collect_gdelt_raw", fake_gdelt)

    out = rlc.run_live_collectors(run_date="2026-05-06")

    expected_keys = {
        "raw/provider=polymarket/date=2026-05-06/live_markets.jsonl",
        "raw/provider=polymarket/date=2026-05-06/live_prices.jsonl",
        "raw/provider=binance/date=2026-05-06/live_klines.jsonl",
        "raw/provider=gdelt/date=2026-05-06/live_timeline.jsonl",
    }
    written_keys = {k for k, _ in written}
    assert expected_keys.issubset(written_keys)

    assert out["raw_polymarket_markets"].endswith("live_markets.jsonl")
    assert out["raw_polymarket_prices"].endswith("live_prices.jsonl")
    assert out["raw_binance_klines"].endswith("live_klines.jsonl")
    assert out["raw_gdelt_timeline"].endswith("live_timeline.jsonl")


def test_run_live_collectors_skips_disabled_sources(monkeypatch) -> None:
    written_keys: list[str] = []

    def fake_write(records, key, bucket=None):  # noqa: ANN001
        written_keys.append(key)

    monkeypatch.setattr(rlc, "write_jsonl_records", fake_write)
    monkeypatch.setattr(
        rlc,
        "collect_polymarket_raw",
        lambda **_kw: ([], [], {}),
    )
    monkeypatch.setattr(rlc, "collect_binance_raw", lambda **_kw: ([], {}))
    monkeypatch.setattr(rlc, "collect_gdelt_raw", lambda **_kw: [])

    out = rlc.run_live_collectors(
        run_date="2026-05-06",
        collect_polymarket=False,
        collect_binance=True,
        collect_gdelt=False,
    )
    assert "raw_polymarket_markets" not in out
    assert "raw_polymarket_prices" not in out
    assert "raw_gdelt_timeline" not in out
    assert "raw_binance_klines" in out
    assert all("provider=binance" in k for k in written_keys)


def test_run_live_collectors_default_gdelt_dates_use_previous_day(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_gdelt(  # noqa: ANN001
        start_date, end_date, narratives_config_path="config/narratives.yaml", **kwargs
    ):
        captured["start"] = start_date
        captured["end"] = end_date
        return []

    monkeypatch.setattr(rlc, "write_jsonl_records", lambda *_a, **_k: None)
    monkeypatch.setattr(rlc, "collect_gdelt_raw", fake_gdelt)
    monkeypatch.setattr(rlc, "collect_polymarket_raw", lambda **_kw: ([], []))
    monkeypatch.setattr(rlc, "collect_binance_raw", lambda **_kw: [])

    rlc.run_live_collectors(
        run_date="2026-05-06",
        collect_polymarket=False,
        collect_binance=False,
        collect_gdelt=True,
    )
    assert captured == {"start": "2026-05-05", "end": "2026-05-06"}
