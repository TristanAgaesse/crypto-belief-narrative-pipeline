from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import crypto_belief_pipeline.quality.issues as qi
from crypto_belief_pipeline.lake.read import LakeKeyNotFound


def _patch_silver_partition_reader(monkeypatch, silver_returns) -> None:
    """Patch the partition reader used for silver datasets in `qi`."""

    def fake(prefix: str, bucket=None) -> pl.DataFrame:
        return silver_returns(prefix)

    monkeypatch.setattr(qi, "read_parquet_partition_df", fake)


def _patch_gold_reader(monkeypatch, gold_returns) -> None:
    """Patch the single-file reader used for gold datasets in `qi`."""

    def fake(key: str, bucket=None) -> pl.DataFrame:
        return gold_returns(key)

    monkeypatch.setattr(qi, "read_parquet_df", fake)


def test_empty_narrative_counts_creates_high_issue(monkeypatch) -> None:
    def silver(prefix: str) -> pl.DataFrame:
        if "silver/narrative_counts" in prefix:
            return pl.DataFrame()
        return pl.DataFrame({"x": [1]})

    def gold(key: str) -> pl.DataFrame:
        return pl.DataFrame({"x": [1]})

    _patch_silver_partition_reader(monkeypatch, silver)
    _patch_gold_reader(monkeypatch, gold)
    monkeypatch.setattr(qi, "load_market_tags", lambda path: pl.DataFrame({"market_id": ["m1"]}))

    issues = qi.detect_data_issues("2026-05-06")
    assert any(i["severity"] == "high" and i["issue"] == "narrative_counts_empty" for i in issues)


def test_empty_candles_creates_critical_issue(monkeypatch) -> None:
    def silver(prefix: str) -> pl.DataFrame:
        if "silver/crypto_candles_1m" in prefix:
            return pl.DataFrame()
        return pl.DataFrame({"x": [1]})

    def gold(key: str) -> pl.DataFrame:
        return pl.DataFrame({"x": [1]})

    _patch_silver_partition_reader(monkeypatch, silver)
    _patch_gold_reader(monkeypatch, gold)
    monkeypatch.setattr(qi, "load_market_tags", lambda path: pl.DataFrame({"market_id": ["m1"]}))

    issues = qi.detect_data_issues("2026-05-06")
    assert any(i["severity"] == "critical" and i["issue"] == "crypto_candles_empty" for i in issues)


def test_high_missing_future_ret_4h_rate_creates_high_issue(monkeypatch) -> None:
    def silver(prefix: str) -> pl.DataFrame:
        return pl.DataFrame({"x": [1]})

    def gold(key: str) -> pl.DataFrame:
        if "gold/training_examples" in key:
            return pl.DataFrame({"future_ret_4h": [None] * 9 + [0.01]})
        return pl.DataFrame({"x": [1]})

    _patch_silver_partition_reader(monkeypatch, silver)
    _patch_gold_reader(monkeypatch, gold)
    monkeypatch.setattr(qi, "load_market_tags", lambda path: pl.DataFrame({"market_id": ["m1"]}))

    issues = qi.detect_data_issues("2026-05-06")
    assert any(
        i["severity"] == "high" and i["issue"] == "missing_future_ret_4h_high" for i in issues
    )


def test_live_signals_empty_creates_info_issue(monkeypatch) -> None:
    def silver(prefix: str) -> pl.DataFrame:
        return pl.DataFrame({"x": [1]})

    def gold(key: str) -> pl.DataFrame:
        if "gold/live_signals" in key:
            return pl.DataFrame()
        if "gold/training_examples" in key:
            return pl.DataFrame({"future_ret_4h": [0.01]})
        return pl.DataFrame({"x": [1]})

    _patch_silver_partition_reader(monkeypatch, silver)
    _patch_gold_reader(monkeypatch, gold)
    monkeypatch.setattr(qi, "load_market_tags", lambda path: pl.DataFrame({"market_id": ["m1"]}))

    issues = qi.detect_data_issues("2026-05-06")
    assert any(i["severity"] == "info" and i["issue"] == "live_signals_empty" for i in issues)


def test_detect_data_issues_reads_partitioned_gold_when_partition_key_provided(monkeypatch) -> None:
    """Dagster hourly gold writes under `partition=.../data.parquet`; issue detection must match."""

    captured_gold_keys: list[str] = []

    def silver(prefix: str) -> pl.DataFrame:
        return pl.DataFrame({"x": [1]})

    def gold(key: str) -> pl.DataFrame:
        captured_gold_keys.append(key)
        # Only the partitioned paths should be used in this test.
        assert "/partition=2026-05-06-12-00/" in key
        return pl.DataFrame({"future_ret_4h": [0.01]})

    _patch_silver_partition_reader(monkeypatch, silver)
    _patch_gold_reader(monkeypatch, gold)
    monkeypatch.setattr(qi, "load_market_tags", lambda path: pl.DataFrame({"market_id": ["m1"]}))

    qi.detect_data_issues("2026-05-06", partition_key="2026-05-06-12:00")
    assert any("gold/training_examples" in k for k in captured_gold_keys)
    assert any("gold/live_signals" in k for k in captured_gold_keys)

def test_missing_partition_returns_empty_not_error(monkeypatch) -> None:
    """Expected not-found is converted to an empty frame and surfaced as an issue."""

    def silver(prefix: str) -> pl.DataFrame:
        raise LakeKeyNotFound(f"missing: {prefix}")

    def gold(key: str) -> pl.DataFrame:
        raise LakeKeyNotFound(f"missing: {key}")

    _patch_silver_partition_reader(monkeypatch, silver)
    _patch_gold_reader(monkeypatch, gold)
    monkeypatch.setattr(qi, "load_market_tags", lambda path: pl.DataFrame({"market_id": ["m1"]}))

    issues = qi.detect_data_issues("2026-05-06")
    issue_codes = {i["issue"] for i in issues}
    assert "crypto_candles_empty" in issue_codes
    assert "belief_price_snapshots_empty" in issue_codes
    assert "narrative_counts_empty" in issue_codes


def test_unexpected_silver_read_failure_propagates(monkeypatch) -> None:
    """Non-not-found read failures must NOT be silently converted to empty data."""

    class _BoomError(RuntimeError):
        pass

    def silver(prefix: str) -> pl.DataFrame:
        raise _BoomError(f"transport failure for {prefix}")

    def gold(key: str) -> pl.DataFrame:
        return pl.DataFrame({"x": [1]})

    _patch_silver_partition_reader(monkeypatch, silver)
    _patch_gold_reader(monkeypatch, gold)
    monkeypatch.setattr(qi, "load_market_tags", lambda path: pl.DataFrame({"market_id": ["m1"]}))

    with pytest.raises(_BoomError):
        qi.detect_data_issues("2026-05-06")


def test_reports_are_written(tmp_path) -> None:
    issues = [
        {
            "severity": "high",
            "category": "source_availability",
            "source": "gdelt",
            "issue": "narrative_counts_empty",
            "message": "No rows",
            "suggested_action": "Do something",
        }
    ]
    md = tmp_path / "issues.md"
    js = tmp_path / "issues.json"
    out = qi.write_data_issues_reports(issues, md_path=md, json_path=js)
    assert Path(out["md"]).exists()
    assert Path(out["json"]).exists()
