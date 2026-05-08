from __future__ import annotations

import pytest

dagster = pytest.importorskip("dagster")
Definitions = dagster.Definitions


def test_definitions_import_and_contents() -> None:
    from crypto_belief_pipeline.orchestration.definitions import defs

    assert isinstance(defs, Definitions)
    assert defs.resolve_job_def("full_stack__minute__manual_job") is not None
    assert defs.resolve_job_def("full_stack__hourly__manual_job") is not None
    assert defs.resolve_job_def("raw_staging__binance__1m_job") is not None
    assert defs.resolve_job_def("raw_staging__polymarket__5m_job") is not None
    assert defs.resolve_job_def("raw_staging__polymarket_discovery__6h_job") is not None
    assert defs.resolve_job_def("raw_staging__gdelt__1h_job") is not None
    assert defs.resolve_job_def("raw_staging__kalshi__5m_job") is not None
    assert defs.resolve_job_def("raw_to_silver__binance__1m_job") is not None
    assert defs.resolve_job_def("raw_to_silver__polymarket__5m_job") is not None
    assert defs.resolve_job_def("raw_to_silver__polymarket_discovery__6h_job") is not None
    assert defs.resolve_job_def("raw_to_silver__gdelt__1h_job") is not None
    assert defs.resolve_job_def("raw_to_silver__kalshi__5m_job") is not None
    assert defs.resolve_job_def("silver_to_gold__signals__5m_job") is not None
    assert defs.resolve_job_def("gold__label_maturation__1h_job") is not None
    assert defs.resolve_job_def("gold_to_quality__hourly_job") is not None
    assert defs.resolve_job_def("quality_to_reports__daily_job") is not None

    repo = defs.get_repository_def()
    schedules = {s.name for s in repo.schedule_defs}
    assert {
        "raw_staging__binance__1m_schedule",
        "raw_staging__polymarket__5m_schedule",
        "raw_staging__polymarket_discovery__6h_schedule",
        "raw_staging__gdelt__1h_schedule",
        "raw_staging__kalshi__5m_schedule",
        "raw_to_silver__binance__1m_schedule",
        "raw_to_silver__polymarket__5m_schedule",
        "raw_to_silver__polymarket_discovery__6h_schedule",
        "raw_to_silver__gdelt__1h_schedule",
        "raw_to_silver__kalshi__5m_schedule",
        "silver_to_gold__signals__5m_schedule",
        "gold__label_maturation__1h_schedule",
        "gold_to_quality__hourly_schedule",
        "quality_to_reports__daily_schedule",
    }.issubset(schedules)
    assert repo.sensor_defs == []

    # Asset keys we expect to exist (names as shown in Dagster UI)
    asset_keys = {k.to_user_string() for k in defs.resolve_all_asset_keys()}
    expected = {
        "raw_sample_inputs",
        "raw_polymarket_staging",
        "raw_binance_staging",
        "raw_gdelt_staging",
        "raw_kalshi_staging",
        "raw_polymarket",
        "raw_binance",
        "raw_gdelt",
        "raw_kalshi",
        "bronze_polymarket",
        "bronze_binance",
        "bronze_gdelt",
        "bronze_kalshi",
        "silver_belief_price_snapshots",
        "silver_crypto_candles_1m",
        "silver_narrative_counts",
        "silver_kalshi_markets",
        "silver_kalshi_market_snapshots",
        "silver_kalshi_events",
        "silver_kalshi_series",
        "silver_kalshi_trades",
        "silver_kalshi_orderbook_snapshots",
        "silver_kalshi_candlesticks",
        "silver_kalshi_event_repricing_features",
        "gold_training_examples",
        "gold_live_signals",
        "soda_data_quality",
        "processing_gaps",
        "data_issues",
        "markdown_reports",
    }
    assert expected.issubset(asset_keys)

    from crypto_belief_pipeline.orchestration.jobs import FULL_STACK_HOURLY_ASSET_NAMES

    assert "silver_narrative_counts" in FULL_STACK_HOURLY_ASSET_NAMES
