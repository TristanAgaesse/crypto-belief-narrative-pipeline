from __future__ import annotations

from pathlib import Path

import polars as pl

import crypto_belief_pipeline.quality.issues as qi


def test_empty_narrative_counts_creates_high_issue(monkeypatch) -> None:
    def fake_read_parquet_df(key: str, bucket=None) -> pl.DataFrame:
        if "silver/narrative_counts" in key:
            return pl.DataFrame()
        return pl.DataFrame({"x": [1]})

    monkeypatch.setattr(qi, "read_parquet_df", fake_read_parquet_df)
    monkeypatch.setattr(qi, "load_market_tags", lambda path: pl.DataFrame({"market_id": ["m1"]}))

    issues = qi.detect_data_issues("2026-05-06")
    assert any(i["severity"] == "high" and i["issue"] == "narrative_counts_empty" for i in issues)


def test_empty_candles_creates_critical_issue(monkeypatch) -> None:
    def fake_read_parquet_df(key: str, bucket=None) -> pl.DataFrame:
        if "silver/crypto_candles_1m" in key:
            return pl.DataFrame()
        return pl.DataFrame({"x": [1]})

    monkeypatch.setattr(qi, "read_parquet_df", fake_read_parquet_df)
    monkeypatch.setattr(qi, "load_market_tags", lambda path: pl.DataFrame({"market_id": ["m1"]}))

    issues = qi.detect_data_issues("2026-05-06")
    assert any(i["severity"] == "critical" and i["issue"] == "crypto_candles_empty" for i in issues)


def test_high_missing_future_ret_4h_rate_creates_high_issue(monkeypatch) -> None:
    def fake_read_parquet_df(key: str, bucket=None) -> pl.DataFrame:
        if "gold/training_examples" in key:
            return pl.DataFrame({"future_ret_4h": [None] * 9 + [0.01]})
        return pl.DataFrame({"x": [1]})

    monkeypatch.setattr(qi, "read_parquet_df", fake_read_parquet_df)
    monkeypatch.setattr(qi, "load_market_tags", lambda path: pl.DataFrame({"market_id": ["m1"]}))

    issues = qi.detect_data_issues("2026-05-06")
    assert any(
        i["severity"] == "high" and i["issue"] == "missing_future_ret_4h_high" for i in issues
    )


def test_live_signals_empty_creates_info_issue(monkeypatch) -> None:
    def fake_read_parquet_df(key: str, bucket=None) -> pl.DataFrame:
        if "gold/live_signals" in key:
            return pl.DataFrame()
        if "gold/training_examples" in key:
            return pl.DataFrame({"future_ret_4h": [0.01]})
        return pl.DataFrame({"x": [1]})

    monkeypatch.setattr(qi, "read_parquet_df", fake_read_parquet_df)
    monkeypatch.setattr(qi, "load_market_tags", lambda path: pl.DataFrame({"market_id": ["m1"]}))

    issues = qi.detect_data_issues("2026-05-06")
    assert any(i["severity"] == "info" and i["issue"] == "live_signals_empty" for i in issues)


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
