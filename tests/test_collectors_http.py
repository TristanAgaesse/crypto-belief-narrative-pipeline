from __future__ import annotations

import httpx
import pytest

from crypto_belief_pipeline.collectors import http as http_mod


def _make_response(
    status_code: int,
    json_body: dict | list | None = None,
    text: str = "",
) -> httpx.Response:
    if json_body is not None:
        return httpx.Response(status_code, json=json_body)
    return httpx.Response(status_code, text=text)


def test_get_json_returns_parsed_payload_on_2xx(monkeypatch) -> None:
    payload = [{"a": 1}, {"a": 2}]

    def handler(request: httpx.Request) -> httpx.Response:
        return _make_response(200, json_body=payload)

    transport = httpx.MockTransport(handler)
    real_client_cls = httpx.Client

    def fake_client(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client_cls(*args, **kwargs)

    monkeypatch.setattr(http_mod.httpx, "Client", fake_client)
    out = http_mod.get_json("https://example.test/api")
    assert out == payload


def test_get_json_raises_immediately_on_non_retryable_4xx(monkeypatch) -> None:
    """A 400/401/403/404 must NOT be retried — surface the failure quickly."""

    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return _make_response(404, text="not found")

    transport = httpx.MockTransport(handler)
    real_client_cls = httpx.Client

    def fake_client(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client_cls(*args, **kwargs)

    monkeypatch.setattr(http_mod.httpx, "Client", fake_client)

    with pytest.raises(http_mod.HttpError) as exc_info:
        http_mod.get_json("https://example.test/missing")
    assert exc_info.value.status_code == 404
    assert call_count["n"] == 1, "non-retryable 4xx should be hit exactly once"


def test_get_json_retries_on_429_then_succeeds(monkeypatch) -> None:
    """429 (rate-limited) should be retried."""

    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return _make_response(429, text="slow down")
        return _make_response(200, json_body={"ok": True})

    transport = httpx.MockTransport(handler)
    real_client_cls = httpx.Client

    def fake_client(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client_cls(*args, **kwargs)

    monkeypatch.setattr(http_mod.httpx, "Client", fake_client)
    out = http_mod.get_json("https://example.test/api")
    assert out == {"ok": True}
    assert call_count["n"] == 2, "429 should trigger one retry"


def test_get_json_retries_on_5xx_then_fails(monkeypatch) -> None:
    """Persistent 5xx should be retried up to 3 attempts and then fail."""

    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return _make_response(503, text="unavailable")

    transport = httpx.MockTransport(handler)
    real_client_cls = httpx.Client

    def fake_client(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client_cls(*args, **kwargs)

    monkeypatch.setattr(http_mod.httpx, "Client", fake_client)

    with pytest.raises(http_mod.HttpError) as exc_info:
        http_mod.get_json("https://example.test/api")
    assert exc_info.value.status_code == 503
    assert call_count["n"] == 3, "5xx should be retried up to 3 attempts"
