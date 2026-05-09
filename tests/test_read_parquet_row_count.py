"""Tests for :func:`read_parquet_row_count`."""

from __future__ import annotations

import io

import polars as pl

from crypto_belief_pipeline.lake import read as read_mod


def test_read_parquet_row_count_uses_metadata_not_full_decode(monkeypatch) -> None:
    buf = io.BytesIO()
    pl.DataFrame({"a": list(range(50))}).write_parquet(buf)
    raw = buf.getvalue()

    class _Body:
        def read(self) -> bytes:
            return raw

    class _Client:
        def get_object(self, **kwargs: object) -> dict[str, object]:
            return {"Body": _Body()}

    monkeypatch.setattr(read_mod, "get_s3_client", lambda **_: _Client())
    monkeypatch.setattr(read_mod, "full_s3_key", lambda k: k)

    assert read_mod.read_parquet_row_count("gold/x/data.parquet", bucket="any-bucket") == 50
