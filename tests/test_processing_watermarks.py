from __future__ import annotations

import io

import pytest

import crypto_belief_pipeline.state.processing_watermarks as pw


class _Body:
    def __init__(self, text: str) -> None:
        self._buf = io.BytesIO(text.encode("utf-8"))

    def read(self) -> bytes:
        return self._buf.read()


class _Client:
    def __init__(self, payload: str) -> None:
        self._payload = payload

    def get_object(self, *, Bucket: str, Key: str):  # noqa: N802 (boto style)
        return {"Body": _Body(self._payload)}


class _Settings:
    s3_bucket = "bucket"
    s3_prefix = ""


def test_read_processing_watermark_raises_on_corrupt_json(monkeypatch) -> None:
    monkeypatch.setattr(pw, "get_settings", lambda: _Settings())
    monkeypatch.setattr(pw, "get_s3_client", lambda settings: _Client("{not json"))

    with pytest.raises(ValueError, match="Corrupt processing watermark JSON"):
        pw.read_processing_watermark("a", "b", "2026-05-06-12:00")


def test_read_processing_watermark_raises_on_schema_mismatch(monkeypatch) -> None:
    monkeypatch.setattr(pw, "get_settings", lambda: _Settings())
    # Valid JSON but missing required fields
    monkeypatch.setattr(pw, "get_s3_client", lambda settings: _Client('{"x": 1}'))

    with pytest.raises(ValueError, match="Invalid processing watermark schema"):
        pw.read_processing_watermark("a", "b", "2026-05-06-12:00")

