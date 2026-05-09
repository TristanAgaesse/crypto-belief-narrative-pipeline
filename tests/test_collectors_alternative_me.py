from __future__ import annotations

from datetime import UTC, datetime

from crypto_belief_pipeline.collectors.alternative_me import fetch_fear_and_greed


def test_fetch_fear_and_greed_ok_payload(monkeypatch) -> None:
    payload = {
        "name": "Fear and Greed Index",
        "data": [
            {
                "value": "55",
                "value_classification": "Neutral",
                "timestamp": "1715126400",
            }
        ],
        "metadata": {"error": None},
    }

    monkeypatch.setattr(
        "crypto_belief_pipeline.collectors.alternative_me.get_json",
        lambda url, params=None, timeout=20.0: payload,
    )

    res = fetch_fear_and_greed(limit=None)
    assert res.ok is True
    assert res.error is None
    assert res.payload == payload
    assert res.raw_hash is not None
    assert res.fetched_at.tzinfo == UTC


def test_fetch_fear_and_greed_metadata_error_marks_not_ok(monkeypatch) -> None:
    payload = {"data": [], "metadata": {"error": "Rate limit"}}
    monkeypatch.setattr(
        "crypto_belief_pipeline.collectors.alternative_me.get_json",
        lambda url, params=None, timeout=20.0: payload,
    )

    res = fetch_fear_and_greed(limit=30)
    assert res.ok is False
    assert res.error is not None and "metadata.error" in res.error
    assert res.payload == payload


def test_fetch_fear_and_greed_non_dict_payload_is_error(monkeypatch) -> None:
    monkeypatch.setattr(
        "crypto_belief_pipeline.collectors.alternative_me.get_json",
        lambda url, params=None, timeout=20.0: [],
    )
    res = fetch_fear_and_greed(limit=0)
    assert res.ok is False
    assert res.error is not None
