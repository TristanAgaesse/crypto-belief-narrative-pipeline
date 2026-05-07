from __future__ import annotations

import io
import json

import pytest
from botocore.exceptions import ClientError

import crypto_belief_pipeline.state.ingestion_cursors as ic
from crypto_belief_pipeline.config import Settings


def _settings() -> Settings:
    return Settings(
        aws_endpoint_url="http://localhost:9000",
        aws_access_key_id="x",
        aws_secret_access_key="y",
        aws_region="us-east-1",
        s3_bucket="b",
    )


class _FakeBody:
    def __init__(self, payload: bytes) -> None:
        self._buf = io.BytesIO(payload)

    def read(self) -> bytes:
        return self._buf.read()


class _FakeClient:
    def __init__(self, *, payload: bytes | None = None, error: ClientError | None = None) -> None:
        self._payload = payload
        self._error = error

    def get_object(self, Bucket: str, Key: str):  # noqa: N803
        if self._error is not None:
            raise self._error
        assert self._payload is not None
        return {"Body": _FakeBody(self._payload)}


def test_read_cursor_returns_none_for_missing_object(monkeypatch) -> None:
    err = ClientError({"Error": {"Code": "NoSuchKey", "Message": "x"}}, "GetObject")
    monkeypatch.setattr(ic, "get_settings", _settings)
    monkeypatch.setattr(ic, "get_s3_client", lambda settings=None: _FakeClient(error=err))

    out = ic.read_cursor(source="binance", key="BTCUSDT")
    assert out is None


def test_read_cursor_raises_on_unexpected_client_error(monkeypatch) -> None:
    err = ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "GetObject")
    monkeypatch.setattr(ic, "get_settings", _settings)
    monkeypatch.setattr(ic, "get_s3_client", lambda settings=None: _FakeClient(error=err))

    with pytest.raises(ClientError):
        ic.read_cursor(source="binance", key="BTCUSDT")


def test_read_cursor_raises_on_malformed_json(monkeypatch) -> None:
    monkeypatch.setattr(ic, "get_settings", _settings)
    monkeypatch.setattr(
        ic, "get_s3_client", lambda settings=None: _FakeClient(payload=b"not-json{")
    )

    with pytest.raises(ValueError, match="not valid JSON"):
        ic.read_cursor(source="binance", key="BTCUSDT")


def test_read_cursor_raises_on_schema_mismatch(monkeypatch) -> None:
    """A cursor object that parses as JSON but doesn't match the schema is a real failure."""

    bad_payload = json.dumps({"hello": "world"}).encode("utf-8")
    monkeypatch.setattr(ic, "get_settings", _settings)
    monkeypatch.setattr(ic, "get_s3_client", lambda settings=None: _FakeClient(payload=bad_payload))

    with pytest.raises(ValueError, match="schema mismatch"):
        ic.read_cursor(source="binance", key="BTCUSDT")


def test_read_cursor_returns_parsed_object_for_valid_payload(monkeypatch) -> None:
    payload = json.dumps(
        {
            "source": "binance",
            "key": "BTCUSDT",
            "last_successful_event_time": "2026-05-06T10:00:00Z",
            "last_successful_ingestion_time": "2026-05-06T10:01:00Z",
            "last_batch_id": "20260506T100000Z000000",
            "status": "ok",
            "extra": {},
        }
    ).encode("utf-8")
    monkeypatch.setattr(ic, "get_settings", _settings)
    monkeypatch.setattr(ic, "get_s3_client", lambda settings=None: _FakeClient(payload=payload))

    out = ic.read_cursor(source="binance", key="BTCUSDT")
    assert out is not None
    assert out.source == "binance"
    assert out.last_batch_id == "20260506T100000Z000000"
