from __future__ import annotations

from datetime import date

from crypto_belief_pipeline.contracts import BRONZE_FEAR_GREED, SILVER_FEAR_GREED_DAILY
from crypto_belief_pipeline.transform.normalize_fear_greed import (
    normalize_fear_greed_payload_records,
    to_fear_greed_daily,
)


def test_normalize_empty_records_schema() -> None:
    df = normalize_fear_greed_payload_records([])
    assert df.height == 0
    BRONZE_FEAR_GREED.validate(df)


def test_normalize_skips_non_dict_records() -> None:
    df = normalize_fear_greed_payload_records([None, "x", {"payload": "not-a-dict"}])  # type: ignore[list-item]
    assert df.height == 0


def _sample_payload_row(*, value: str | int, ts: str | int, classification: str = "Neutral") -> dict:
    return {
        "source": "alternative_me",
        "fetched_at": "2026-05-06T12:00:00+00:00",
        "ok": True,
        "error": None,
        "payload": {
            "data": [
                {
                    "value": value,
                    "value_classification": classification,
                    "timestamp": ts,
                }
            ],
            "metadata": {"time_until_update": 3600},
        },
    }


def test_normalize_coerces_string_value_and_timestamp() -> None:
    # API often returns value and timestamp as strings.
    df = normalize_fear_greed_payload_records(
        [_sample_payload_row(value="55", ts="1715126400")]
    )
    BRONZE_FEAR_GREED.validate(df)
    assert df.height == 1
    assert df["value"][0] == 55
    assert df["date_utc"][0] == date(2024, 5, 8)


def test_normalize_dedupes_same_calendar_day_by_latest_fetched_at() -> None:
    ts = 1715126400
    older = {
        "source": "alternative_me",
        "fetched_at": "2026-05-06T10:00:00+00:00",
        "ok": True,
        "error": None,
        "payload": {
            "data": [{"value": 40, "value_classification": "Fear", "timestamp": ts}],
            "metadata": {},
        },
    }
    newer = {
        "source": "alternative_me",
        "fetched_at": "2026-05-06T14:00:00+00:00",
        "ok": True,
        "error": None,
        "payload": {
            "data": [{"value": 55, "value_classification": "Neutral", "timestamp": ts}],
            "metadata": {},
        },
    }
    df = normalize_fear_greed_payload_records([older, newer])
    BRONZE_FEAR_GREED.validate(df)
    assert df.height == 1
    assert df["value"][0] == 55
    assert df["value_classification"][0] == "Neutral"


def test_to_fear_greed_daily_contract() -> None:
    bronze = normalize_fear_greed_payload_records(
        [_sample_payload_row(value=60, ts=1715126400)]
    )
    daily = to_fear_greed_daily(bronze)
    SILVER_FEAR_GREED_DAILY.validate(daily)
    assert daily.height == 1
    assert "first_seen_at" in daily.columns


def test_normalize_skips_items_missing_required_fields() -> None:
    rec = {
        "source": "alternative_me",
        "fetched_at": "2026-05-06T12:00:00+00:00",
        "ok": True,
        "error": None,
        "payload": {
            "data": [
                {"value": 10, "value_classification": "Extreme Fear", "timestamp": 1715000000},
                {"value": None, "value_classification": "Neutral", "timestamp": 1715000001},
                {"value": 20, "value_classification": "Fear", "timestamp": "not-a-number"},
            ],
            "metadata": {},
        },
    }
    df = normalize_fear_greed_payload_records([rec])
    assert df.height == 1
    assert df["value"][0] == 10
