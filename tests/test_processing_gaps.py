from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytest.importorskip("dagster")

from crypto_belief_pipeline.orchestration import assets_quality as aq


def test_scan_processing_gaps_for_partition_reads_only_non_ok(monkeypatch) -> None:
    ok = MagicMock()
    ok.status = "ok"
    bad = MagicMock()
    bad.status = "failed"
    bad.source = "binance"
    bad.partition_key = "2026-05-07-11:00"
    bad.last_processed_batch_id = "bid1"

    reads: list[tuple[str, str]] = []

    def _read(*, consumer_asset: str, source: str, partition_key: str):
        reads.append((consumer_asset, source))
        if consumer_asset == "bronze_binance":
            return bad
        return ok

    monkeypatch.setattr(aq, "read_processing_watermark", _read)

    gaps = aq._scan_processing_gaps_for_partition("2026-05-07-11:00")
    assert len(reads) == 4
    assert {r[0] for r in reads} == {
        "bronze_polymarket",
        "bronze_binance",
        "bronze_gdelt",
        "bronze_kalshi",
    }
    assert len(gaps) == 1
    assert gaps[0]["consumer_asset"] == "bronze_binance"
    assert gaps[0]["reason"] == "watermark_status=failed"
    assert gaps[0]["partition_key"] == "2026-05-07-11:00"
