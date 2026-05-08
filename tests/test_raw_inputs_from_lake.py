from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

import crypto_belief_pipeline.orchestration.raw_inputs_from_lake as ri


def test_resolve_polymarket_prefers_latest_microbatch(monkeypatch) -> None:
    keys = [
        "raw/provider=polymarket/date=2026-05-07/hour=10/batch_id=20260507T100000Z_markets.jsonl",
        "raw/provider=polymarket/date=2026-05-07/hour=10/batch_id=20260507T100000Z_prices.jsonl",
        "raw/provider=polymarket/date=2026-05-07/hour=11/batch_id=20260507T110000Z_markets.jsonl",
        "raw/provider=polymarket/date=2026-05-07/hour=11/batch_id=20260507T110000Z_prices.jsonl",
    ]

    monkeypatch.setattr(ri, "list_jsonl_keys_under", lambda prefix, bucket=None: list(keys))
    monkeypatch.setattr(ri, "_candidate_buckets", lambda: ["lake"])

    d, bucket = ri.resolve_raw_polymarket_for_partition(date(2026, 5, 7))
    assert bucket == "lake"
    assert d["raw_polymarket_markets"].endswith("20260507T110000Z_markets.jsonl")
    assert d["raw_polymarket_prices"].endswith("20260507T110000Z_prices.jsonl")
    assert d["source_batch_id"] == "20260507T110000Z"


def test_resolve_polymarket_live_pair(monkeypatch) -> None:
    keys = [
        "raw/provider=polymarket/date=2026-05-07/live_markets.jsonl",
        "raw/provider=polymarket/date=2026-05-07/live_prices.jsonl",
    ]
    monkeypatch.setattr(ri, "list_jsonl_keys_under", lambda prefix, bucket=None: list(keys))
    monkeypatch.setattr(ri, "_candidate_buckets", lambda: ["lake"])

    d, bucket = ri.resolve_raw_polymarket_for_partition(date(2026, 5, 7))
    assert bucket == "lake"
    assert d["raw_polymarket_markets"].endswith("live_markets.jsonl")
    assert d["source_batch_id"] == ""


def test_resolve_binance_micro(monkeypatch) -> None:
    keys = [
        "raw/provider=binance/date=2026-05-07/hour=10/batch_id=20260507T100000Z.jsonl",
        "raw/provider=binance/date=2026-05-07/hour=11/batch_id=20260507T110000Z.jsonl",
    ]
    monkeypatch.setattr(ri, "list_jsonl_keys_under", lambda prefix, bucket=None: list(keys))
    monkeypatch.setattr(ri, "_candidate_buckets", lambda: ["lake"])

    d, bucket = ri.resolve_raw_binance_for_partition(date(2026, 5, 7))
    assert bucket == "lake"
    assert d["raw_binance_klines"].endswith("20260507T110000Z.jsonl")
    assert d["source_batch_id"] == "20260507T110000Z"


def test_resolve_gdelt_fallback_timeline(monkeypatch) -> None:
    monkeypatch.setattr(ri, "list_jsonl_keys_under", lambda prefix, bucket=None: [])
    monkeypatch.setattr(ri, "_candidate_buckets", lambda: ["lake"])

    d, bucket = ri.resolve_raw_gdelt_for_partition(date(2026, 5, 7))
    assert bucket == "lake"
    assert d["raw_gdelt_timeline"] == "raw/provider=gdelt/date=2026-05-07/live_timeline.jsonl"


def test_list_binance_for_partition_window_filters_range(monkeypatch) -> None:
    keys = [
        "raw/provider=binance/date=2026-05-07/hour=10/batch_id=20260507T105959Z.jsonl",
        "raw/provider=binance/date=2026-05-07/hour=11/batch_id=20260507T110000Z.jsonl",
    ]
    monkeypatch.setattr(ri, "list_jsonl_keys_under", lambda prefix, bucket=None: list(keys))
    monkeypatch.setattr(ri, "_candidate_buckets", lambda: ["lake"])
    out, bucket = ri.list_raw_binance_for_partition_window(
        partition_start=datetime(2026, 5, 7, 11, 0, 0, tzinfo=UTC),
        partition_end=datetime(2026, 5, 7, 11, 1, 0, tzinfo=UTC),
    )
    assert bucket == "lake"
    assert len(out) == 1
    assert out[0]["raw_binance_klines"].endswith("20260507T110000Z.jsonl")


def test_unseen_raw_inputs_uses_watermark(monkeypatch) -> None:
    class _WM:
        last_processed_batch_id = "20260507T110000Z"

    monkeypatch.setattr(ri, "read_processing_watermark", lambda **kwargs: _WM())
    out = ri.unseen_raw_inputs_for_partition(
        consumer_asset="bronze_binance",
        source="binance",
        partition_key="2026-05-07T11:00",
        candidate_inputs=[
            {"raw_binance_klines": "a", "source_batch_id": "20260507T110000Z"},
            {"raw_binance_klines": "b", "source_batch_id": "20260507T110010Z"},
        ],
        key_fields=("raw_binance_klines", "source_batch_id"),
    )
    assert len(out) == 1
    assert out[0]["raw_binance_klines"] == "b"


def test_unseen_raw_inputs_does_not_use_lexicographic_batch_order(monkeypatch) -> None:
    class _WM:
        last_processed_batch_id = "20260507T110010Z_z"

    monkeypatch.setattr(ri, "read_processing_watermark", lambda **kwargs: _WM())
    out = ri.unseen_raw_inputs_for_partition(
        consumer_asset="bronze_binance",
        source="binance",
        partition_key="2026-05-07T11:00",
        candidate_inputs=[
            {"raw_binance_klines": "old", "source_batch_id": "20260507T110009Z_zzzz"},
            {"raw_binance_klines": "new", "source_batch_id": "20260507T110011Z_a"},
        ],
        key_fields=("raw_binance_klines", "source_batch_id"),
    )
    assert [it["raw_binance_klines"] for it in out] == ["new"]


def test_unseen_raw_inputs_reprocesses_when_batch_id_malformed(monkeypatch) -> None:
    class _WM:
        last_processed_batch_id = "not-sortable"

    monkeypatch.setattr(ri, "read_processing_watermark", lambda **kwargs: _WM())
    out = ri.unseen_raw_inputs_for_partition(
        consumer_asset="bronze_binance",
        source="binance",
        partition_key="2026-05-07T11:00",
        candidate_inputs=[
            {"raw_binance_klines": "a", "source_batch_id": "also-bad"},
            {"raw_binance_klines": "a", "source_batch_id": "also-bad"},
        ],
        key_fields=("raw_binance_klines", "source_batch_id"),
    )
    assert out == [{"raw_binance_klines": "a", "source_batch_id": "also-bad"}]


def test_unseen_raw_inputs_keeps_same_second_batch_with_larger_suffix(monkeypatch) -> None:
    class _WM:
        last_processed_batch_id = "20260507T110010Z_a"

    monkeypatch.setattr(ri, "read_processing_watermark", lambda **kwargs: _WM())
    out = ri.unseen_raw_inputs_for_partition(
        consumer_asset="bronze_binance",
        source="binance",
        partition_key="2026-05-07T11:00",
        candidate_inputs=[
            {"raw_binance_klines": "seen", "source_batch_id": "20260507T110010Z_a"},
            {"raw_binance_klines": "newer", "source_batch_id": "20260507T110010Z_b"},
        ],
        key_fields=("raw_binance_klines", "source_batch_id"),
    )
    assert [it["raw_binance_klines"] for it in out] == ["newer"]


def test_list_binance_for_partition_window_spans_date_boundary(monkeypatch) -> None:
    keys_by_prefix = {
        "raw/provider=binance/date=2026-05-07": [
            "raw/provider=binance/date=2026-05-07/hour=23/batch_id=20260507T235959Z.jsonl"
        ],
        "raw/provider=binance/date=2026-05-08": [
            "raw/provider=binance/date=2026-05-08/hour=00/batch_id=20260508T000000Z.jsonl"
        ],
    }

    def _list(prefix, bucket=None):
        return list(keys_by_prefix.get(prefix, []))

    monkeypatch.setattr(ri, "list_jsonl_keys_under", _list)
    monkeypatch.setattr(ri, "_candidate_buckets", lambda: ["lake"])
    out, bucket = ri.list_raw_binance_for_partition_window(
        partition_start=datetime(2026, 5, 7, 23, 59, 0, tzinfo=UTC),
        partition_end=datetime(2026, 5, 8, 0, 1, 0, tzinfo=UTC),
    )
    assert bucket == "lake"
    assert len(out) == 2
    assert out[0]["source_batch_id"] == "20260507T235959Z"
    assert out[1]["source_batch_id"] == "20260508T000000Z"


@pytest.mark.parametrize(
    "fn_name",
    [
        "list_raw_polymarket_for_partition_window",
        "list_raw_binance_for_partition_window",
        "list_raw_gdelt_for_partition_window",
    ],
)
def test_list_raw_window_final_raise_includes_window_not_nameerror(
    monkeypatch, fn_name: str
) -> None:
    monkeypatch.setattr(ri, "_candidate_buckets", lambda: [])
    fn = getattr(ri, fn_name)
    start = datetime(2026, 5, 7, 11, 0, 0, tzinfo=UTC)
    end = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
    with pytest.raises(FileNotFoundError, match=r"window=\[") as excinfo:
        fn(partition_start=start, partition_end=end)
    assert "NameError" not in str(excinfo.value)
