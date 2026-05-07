from __future__ import annotations

import polars as pl
import pytest

from crypto_belief_pipeline.features.scoring import (
    add_candidate_flag,
    add_penalties,
    add_underreaction_score,
)


def _row(**overrides: object) -> dict:
    base = {
        "belief_shock_abs_1h": 0.10,
        "narrative_z_1h": 0.0,
        "directional_price_reaction_1h": 0.0,
        "spread": None,
        "liquidity": 100000.0,
        "confidence": 0.9,
        "relevance": "high",
        "future_ret_1h": 0.0,
        "future_ret_4h": 0.0,
        "future_ret_24h": 0.0,
        "asset_ret_past_1h": 0.0,
    }
    base.update(overrides)
    return base


def _df(**overrides: object) -> pl.DataFrame:
    return pl.DataFrame([_row(**overrides)])


def test_underreaction_score_baseline() -> None:
    df = _df(belief_shock_abs_1h=0.10)
    out = add_underreaction_score(add_penalties(df))
    assert out["underreaction_score"][0] == pytest.approx(0.20)


def test_underreaction_score_penalizes_already_priced_directional_move() -> None:
    plain = add_underreaction_score(add_penalties(_df(directional_price_reaction_1h=0.0)))
    priced = add_underreaction_score(add_penalties(_df(directional_price_reaction_1h=0.05)))
    assert priced["underreaction_score"][0] < plain["underreaction_score"][0]
    assert priced["underreaction_score"][0] == pytest.approx(0.20 - 1.5 * 0.05)


def test_underreaction_score_rewards_narrative_acceleration() -> None:
    plain = add_underreaction_score(add_penalties(_df(narrative_z_1h=0.0)))
    accel = add_underreaction_score(add_penalties(_df(narrative_z_1h=2.0)))
    assert accel["underreaction_score"][0] > plain["underreaction_score"][0]
    assert accel["underreaction_score"][0] == pytest.approx(0.20 + 2.0)


def test_negative_directional_price_reaction_does_not_subtract() -> None:
    out = add_underreaction_score(add_penalties(_df(directional_price_reaction_1h=-0.05)))
    assert out["underreaction_score"][0] == pytest.approx(0.20)


def test_spread_penalty_clamped_to_one() -> None:
    out = add_penalties(_df(spread=0.5))
    assert out["spread_penalty"][0] == pytest.approx(1.0)


def test_spread_penalty_null_is_zero() -> None:
    out = add_penalties(_df(spread=None))
    assert out["spread_penalty"][0] == pytest.approx(0.0)


def test_illiquidity_penalty_levels() -> None:
    null_liq = add_penalties(_df(liquidity=None))
    low_liq = add_penalties(_df(liquidity=500.0))
    ok_liq = add_penalties(_df(liquidity=10000.0))
    assert null_liq["illiquidity_penalty"][0] == pytest.approx(1.0)
    assert low_liq["illiquidity_penalty"][0] == pytest.approx(0.5)
    assert ok_liq["illiquidity_penalty"][0] == pytest.approx(0.0)


def test_candidate_flag_requires_threshold_and_relevance() -> None:
    df = _df(belief_shock_abs_1h=0.10, confidence=0.9, relevance="high")
    out = add_candidate_flag(add_underreaction_score(add_penalties(df)))
    assert out["is_candidate_event"][0] is True


def test_candidate_flag_blocked_by_low_confidence() -> None:
    df = _df(belief_shock_abs_1h=0.10, confidence=0.4, relevance="high")
    out = add_candidate_flag(add_underreaction_score(add_penalties(df)))
    assert out["is_candidate_event"][0] is False


def test_candidate_flag_blocked_by_low_relevance() -> None:
    df = _df(belief_shock_abs_1h=0.10, confidence=0.9, relevance="low")
    out = add_candidate_flag(add_underreaction_score(add_penalties(df)))
    assert out["is_candidate_event"][0] is False


def test_candidate_flag_blocked_by_small_belief_shock() -> None:
    df = _df(belief_shock_abs_1h=0.01, confidence=0.9, relevance="high")
    out = add_candidate_flag(add_underreaction_score(add_penalties(df)))
    assert out["is_candidate_event"][0] is False


def test_quality_flags_concatenates_with_pipe() -> None:
    df = _df(
        narrative_z_1h=None,
        asset_ret_past_1h=None,
        future_ret_1h=None,
        future_ret_4h=None,
        future_ret_24h=None,
    )
    out = add_candidate_flag(add_underreaction_score(add_penalties(df)))
    flags = out["quality_flags"][0]
    assert "missing_narrative" in flags
    assert "missing_price_lag" in flags
    assert "missing_future_labels" in flags
    assert flags.count("|") == 2


def test_quality_flags_empty_when_all_present() -> None:
    df = _df(
        narrative_z_1h=0.5,
        asset_ret_past_1h=0.01,
        future_ret_1h=0.02,
        future_ret_4h=0.03,
        future_ret_24h=0.04,
    )
    out = add_candidate_flag(add_underreaction_score(add_penalties(df)))
    assert out["quality_flags"][0] == ""
