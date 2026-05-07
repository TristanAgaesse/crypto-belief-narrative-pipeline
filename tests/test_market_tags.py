from __future__ import annotations

import polars as pl
import pytest

from crypto_belief_pipeline.features.market_tags import (
    DEFAULT_MARKET_TAGS_PATH,
    load_market_tags,
    validate_market_tags,
)


def _valid_row(**overrides: object) -> dict:
    base = {
        "market_id": "m1",
        "asset": "BTC",
        "narrative": "bitcoin_reserve",
        "direction": 1,
        "relevance": "high",
        "confidence": 0.9,
        "notes": "",
    }
    base.update(overrides)
    return base


def test_load_market_tags_default_path_returns_required_columns() -> None:
    df = load_market_tags(DEFAULT_MARKET_TAGS_PATH)
    expected = {
        "market_id",
        "asset",
        "narrative",
        "direction",
        "relevance",
        "confidence",
        "notes",
    }
    assert expected.issubset(set(df.columns))
    assert df.height >= 1


def test_validate_market_tags_accepts_valid_rows() -> None:
    df = pl.DataFrame([_valid_row()])
    out = validate_market_tags(df)
    assert out.height == 1
    assert out.schema["direction"] == pl.Int64
    assert out.schema["confidence"] == pl.Float64
    assert out.schema["asset"] == pl.String


def test_validate_market_tags_rejects_invalid_direction() -> None:
    df = pl.DataFrame([_valid_row(direction=0)])
    with pytest.raises(ValueError, match="invalid direction"):
        validate_market_tags(df)


def test_validate_market_tags_rejects_invalid_asset() -> None:
    df = pl.DataFrame([_valid_row(asset="DOGE")])
    with pytest.raises(ValueError, match="invalid asset"):
        validate_market_tags(df)


def test_validate_market_tags_rejects_invalid_relevance() -> None:
    df = pl.DataFrame([_valid_row(relevance="critical")])
    with pytest.raises(ValueError, match="invalid relevance"):
        validate_market_tags(df)


def test_validate_market_tags_rejects_invalid_confidence() -> None:
    df = pl.DataFrame([_valid_row(confidence=1.5)])
    with pytest.raises(ValueError, match="invalid confidence"):
        validate_market_tags(df)


def test_validate_market_tags_rejects_duplicate_keys() -> None:
    df = pl.DataFrame([_valid_row(), _valid_row()])
    with pytest.raises(ValueError, match="duplicate"):
        validate_market_tags(df)


def test_validate_market_tags_rejects_missing_columns() -> None:
    df = pl.DataFrame({"market_id": ["m1"], "asset": ["BTC"]})
    with pytest.raises(ValueError, match="missing required columns"):
        validate_market_tags(df)
