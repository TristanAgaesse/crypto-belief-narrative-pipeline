from __future__ import annotations

import polars as pl

import crypto_belief_pipeline.lake.compaction as comp


def test_compact_hourly_microbatches_empty_returns_none(monkeypatch) -> None:
    monkeypatch.setattr(comp, "_list_keys", lambda prefix: [])
    out = comp.compact_hourly_microbatches(
        layer="silver",
        dataset="x",
        run_date="2026-05-06",
        hour=10,
    )
    assert out is None


def test_compact_hourly_microbatches_writes_one_file(monkeypatch) -> None:
    keys = [
        "silver/x/date=2026-05-06/hour=10/batch_id=1.parquet",
        "silver/x/date=2026-05-06/hour=10/batch_id=2.parquet",
    ]
    monkeypatch.setattr(comp, "_list_keys", lambda prefix: keys)
    monkeypatch.setattr(comp, "_read_parquet_keys", lambda ks: pl.DataFrame({"x": [1, 2]}))

    written: list[str] = []
    monkeypatch.setattr(comp, "write_parquet_df", lambda df, key: written.append(key))

    out = comp.compact_hourly_microbatches(
        layer="silver",
        dataset="x",
        run_date="2026-05-06",
        hour=10,
    )
    assert out is not None
    assert out.input_files == 2
    assert out.output_key.endswith("/data.parquet")
    assert written == [out.output_key]


def test_compact_daily_from_hourly_empty_returns_none(monkeypatch) -> None:
    monkeypatch.setattr(comp, "_list_keys", lambda prefix: [])
    out = comp.compact_daily_from_hourly(
        layer="silver",
        dataset="x",
        run_date="2026-05-06",
    )
    assert out is None
