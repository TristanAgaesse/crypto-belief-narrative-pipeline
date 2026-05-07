from __future__ import annotations

import pytest

dagster = pytest.importorskip("dagster")
Definitions = dagster.Definitions


def test_definitions_import_and_contents() -> None:
    from crypto_belief_pipeline.orchestration.definitions import defs

    assert isinstance(defs, Definitions)
    assert defs.resolve_job_def("incremental_live_job") is not None
    assert defs.resolve_job_def("live_market_fast_job") is not None
    assert defs.resolve_job_def("full_refresh_job") is not None
    assert defs.resolve_job_def("binance_raw_1m_job") is not None
    assert defs.resolve_job_def("polymarket_prices_5m_job") is not None
    assert defs.resolve_job_def("polymarket_discovery_6h_job") is not None
    assert defs.resolve_job_def("gdelt_raw_1h_job") is not None
    assert defs.resolve_job_def("silver_microbatch_5m_job") is not None
    assert defs.resolve_job_def("gold_live_signals_5m_job") is not None
    assert defs.resolve_job_def("label_maturation_1h_job") is not None
    assert defs.resolve_job_def("soda_60m_job") is not None
    assert defs.resolve_job_def("reports_daily_job") is not None

    repo = defs.get_repository_def()
    schedules = {s.name for s in repo.schedule_defs}
    assert {
        "live_market_fast_schedule",
        "live_research_hourly_schedule",
        "binance_raw_1m_schedule",
        "polymarket_prices_5m_schedule",
        "polymarket_discovery_6h_schedule",
        "gdelt_raw_1h_schedule",
        "silver_microbatch_5m_schedule",
        "gold_live_signals_5m_schedule",
        "label_maturation_1h_schedule",
        "soda_60m_schedule",
        "reports_daily_schedule",
    }.issubset(schedules)

    # Asset keys we expect to exist (names as shown in Dagster UI)
    asset_keys = {k.to_user_string() for k in defs.resolve_all_asset_keys()}
    expected = {
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
