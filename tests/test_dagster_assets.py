from __future__ import annotations

import pytest

dagster = pytest.importorskip("dagster")
Definitions = dagster.Definitions


def test_definitions_import_and_contents() -> None:
    from crypto_belief_pipeline.orchestration.definitions import defs

    assert isinstance(defs, Definitions)
    assert defs.resolve_job_def("full_stack__sample__manual_job") is not None
    assert defs.resolve_job_def("full_stack__live__hourly_job") is not None
    assert defs.resolve_job_def("raw__market_fast__5m_job") is not None
    assert defs.resolve_job_def("full_refresh__all_layers__manual_job") is not None
    assert defs.resolve_job_def("raw_to_silver__binance__1m_job") is not None
    assert defs.resolve_job_def("raw_to_silver__polymarket__5m_job") is not None
    assert defs.resolve_job_def("raw_to_silver__polymarket_discovery__6h_job") is not None
    assert defs.resolve_job_def("raw_to_silver__gdelt__1h_job") is not None
    assert defs.resolve_job_def("silver_to_gold__signals__5m_job") is not None
    assert defs.resolve_job_def("gold__label_maturation__1h_job") is not None
    assert defs.resolve_job_def("gold_to_quality__hourly_job") is not None
    assert defs.resolve_job_def("quality_to_reports__daily_job") is not None

    repo = defs.get_repository_def()
    schedules = {s.name for s in repo.schedule_defs}
    assert {
        "raw__market_fast__5m_schedule",
        "full_stack__live__hourly_schedule",
        "raw_to_silver__binance__1m_schedule",
        "raw_to_silver__polymarket__5m_schedule",
        "raw_to_silver__polymarket_discovery__6h_schedule",
        "raw_to_silver__gdelt__1h_schedule",
        "silver_to_gold__signals__5m_schedule",
        "gold__label_maturation__1h_schedule",
        "gold_to_quality__hourly_schedule",
        "quality_to_reports__daily_schedule",
    }.issubset(schedules)

    # Asset keys we expect to exist (names as shown in Dagster UI)
    asset_keys = {k.to_user_string() for k in defs.resolve_all_asset_keys()}
    expected = {
        "raw_sample_inputs",
        "raw_polymarket",
        "raw_binance",
        "raw_gdelt",
        "bronze_polymarket",
        "bronze_binance",
        "bronze_gdelt",
        "silver_belief_price_snapshots",
        "silver_crypto_candles_1m",
        "silver_narrative_counts",
        "gold_training_examples",
        "gold_live_signals",
        "soda_data_quality",
        "data_issues",
        "markdown_reports",
    }
    assert expected.issubset(asset_keys)
