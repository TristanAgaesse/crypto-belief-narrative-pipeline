from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

pytest.importorskip("dagster")

from crypto_belief_pipeline.orchestration.assets_raw import (
    raw_binance_hourly,
    raw_gdelt_hourly,
    raw_polymarket_hourly,
)


class _Ctx:
    def __init__(self, partition_key: str, start: datetime, end: datetime) -> None:
        self.has_partition_key = True
        self.partition_key = partition_key
        self.partition_time_window = SimpleNamespace(start=start, end=end)
        self.metadata: list[dict] = []

    def add_output_metadata(self, md: dict) -> None:
        self.metadata.append(md)


def _asset_fn(asset_def):
    return asset_def.op.compute_fn.decorated_fn


def test_raw_binance_hourly_falls_back_to_api_when_staging_missing(monkeypatch) -> None:
    start = datetime(2026, 5, 7, 11, 0, 0, tzinfo=UTC)
    end = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
    ctx = _Ctx("2026-05-07-11:00", start, end)

    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw.list_raw_binance_for_partition_window",
        lambda **kwargs: ([], "lake"),
    )

    called_limit = 0
    called_start: datetime | None = None
    called_end: datetime | None = None

    def _collect(*, limit: int, start_time: datetime, end_time: datetime):
        nonlocal called_limit, called_start, called_end
        called_limit = limit
        called_start = start_time
        called_end = end_time
        return ([{"symbol": "BTCUSDT", "open_time": "2026-05-07T11:00:00Z"}], {})

    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw.collect_binance_raw",
        _collect,
    )

    writes: list[tuple[list[dict], str, str | None]] = []

    def _write_jsonl_records(records, key, bucket=None):
        writes.append((list(records), key, bucket))

    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw.write_jsonl_records",
        _write_jsonl_records,
    )

    out = _asset_fn(raw_binance_hourly)(ctx)

    assert called_limit == 1000
    assert called_start == start
    assert called_end == end
    assert len(writes) == 1
    assert out["raw_binance_klines"].endswith("/partition=2026-05-07-11-00/data.jsonl")
    assert writes[0][1] == out["raw_binance_klines"]
    assert writes[0][0] == [{"symbol": "BTCUSDT", "open_time": "2026-05-07T11:00:00Z"}]
    assert writes[0][2] == "lake"
    assert ctx.metadata
    assert ctx.metadata[-1]["data_source"] == "api_backfill"
    assert ctx.metadata[-1]["fallback_triggered_for_missing_microbatches"] is True


def test_raw_binance_hourly_falls_back_when_hour_has_missing_slots(monkeypatch) -> None:
    start = datetime(2026, 5, 7, 11, 0, 0, tzinfo=UTC)
    end = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
    ctx = _Ctx("2026-05-07-11:00", start, end)

    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw.get_runtime_config",
        lambda: SimpleNamespace(cadence=SimpleNamespace(binance_raw_seconds=60)),
    )
    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw.list_raw_binance_for_partition_window",
        lambda **kwargs: (
            [
                {
                    "raw_binance_klines": (
                        "raw/provider=binance/date=2026-05-07/hour=11/"
                        "batch_id=20260507T110000Z.jsonl"
                    ),
                    "source_batch_id": "20260507T110000Z",
                    "source_window_start": "",
                    "source_window_end": "",
                }
            ],
            "lake",
        ),
    )

    api_called = False

    def _collect(*, limit: int, start_time: datetime, end_time: datetime):
        nonlocal api_called
        api_called = True
        return ([{"symbol": "ETHUSDT", "open_time": "2026-05-07T11:00:00Z"}], {})

    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw.collect_binance_raw",
        _collect,
    )
    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw.write_jsonl_records",
        lambda records, key, bucket=None: None,
    )

    _asset_fn(raw_binance_hourly)(ctx)
    assert api_called is True


def test_raw_gdelt_hourly_falls_back_to_api_when_staging_missing(monkeypatch) -> None:
    start = datetime(2026, 5, 7, 11, 0, 0, tzinfo=UTC)
    end = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
    ctx = _Ctx("2026-05-07-11:00", start, end)

    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw.list_raw_gdelt_for_partition_window",
        lambda **kwargs: ([], "lake"),
    )

    called_start: datetime | None = None
    called_end: datetime | None = None

    def _collect(*, start_time: datetime, end_time: datetime):
        nonlocal called_start, called_end
        called_start = start_time
        called_end = end_time
        return ([{"narrative": "btc", "timestamp": "2026-05-07T11:00:00Z"}], {})

    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw.collect_gdelt_raw_window",
        _collect,
    )

    writes: list[tuple[list[dict], str, str | None]] = []

    def _write_jsonl_records(records, key, bucket=None):
        writes.append((list(records), key, bucket))

    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw.write_jsonl_records",
        _write_jsonl_records,
    )

    out = _asset_fn(raw_gdelt_hourly)(ctx)

    assert called_start == start
    assert called_end == end
    assert len(writes) == 1
    assert out["raw_gdelt_timeline"].endswith("/partition=2026-05-07-11-00/data.jsonl")
    assert writes[0][2] == "lake"
    assert ctx.metadata
    assert ctx.metadata[-1]["data_source"] == "api_backfill"
    assert ctx.metadata[-1]["fallback_triggered_for_missing_microbatches"] is True


def test_raw_polymarket_hourly_falls_back_to_api_when_staging_missing(monkeypatch) -> None:
    start = datetime(2026, 5, 7, 11, 0, 0, tzinfo=UTC)
    end = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
    ctx = _Ctx("2026-05-07-11:00", start, end)

    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw.list_raw_polymarket_for_partition_window",
        lambda **kwargs: ([], "lake"),
    )
    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw._read_yaml_mapping",
        lambda path: {
            "polymarket": {
                "limit": 25,
                "keywords": ["btc"],
                "active": True,
                "closed": False,
            }
        },
    )

    called_limit = 0
    called_start: datetime | None = None
    called_end: datetime | None = None
    called_snapshot: datetime | None = None

    def _collect(
        *,
        limit: int,
        keywords: list[str],
        start_time: datetime,
        end_time: datetime,
        snapshot_time: datetime,
        active: bool,
        closed: bool,
    ):
        nonlocal called_limit, called_start, called_end, called_snapshot
        called_limit = limit
        called_start = start_time
        called_end = end_time
        called_snapshot = snapshot_time
        return (
            [{"market_id": "m1"}],
            [{"market_id": "m1", "outcome": "YES"}],
            {},
        )

    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw.collect_polymarket_raw",
        _collect,
    )

    writes: list[tuple[list[dict], str, str | None]] = []

    def _write_jsonl_records(records, key, bucket=None):
        writes.append((list(records), key, bucket))

    monkeypatch.setattr(
        "crypto_belief_pipeline.orchestration.assets_raw.write_jsonl_records",
        _write_jsonl_records,
    )

    out = _asset_fn(raw_polymarket_hourly)(ctx)

    assert called_limit == 25
    assert called_start == start
    assert called_end == end
    assert called_snapshot == end - timedelta(seconds=1)
    assert len(writes) == 2
    assert "provider=polymarket_markets" in out["raw_polymarket_markets"]
    assert "provider=polymarket_prices" in out["raw_polymarket_prices"]
    assert out["raw_polymarket_markets"].endswith("/partition=2026-05-07-11-00/data.jsonl")
    assert out["raw_polymarket_prices"].endswith("/partition=2026-05-07-11-00/data.jsonl")
    assert writes[0][2] == "lake"
    assert writes[1][2] == "lake"
    assert ctx.metadata
    assert ctx.metadata[-1]["data_source"] == "api_backfill"
    assert ctx.metadata[-1]["fallback_triggered_for_missing_microbatches"] is True
