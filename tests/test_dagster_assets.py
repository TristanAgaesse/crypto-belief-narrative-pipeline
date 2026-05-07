from __future__ import annotations

from dagster import Definitions


def test_definitions_import_and_contents() -> None:
    from crypto_belief_pipeline.orchestration.definitions import defs

    assert isinstance(defs, Definitions)
    assert defs.resolve_job_def("incremental_sample_job") is not None
    assert defs.resolve_job_def("incremental_live_job") is not None
    assert defs.resolve_job_def("live_market_fast_job") is not None
    assert defs.resolve_job_def("full_refresh_job") is not None

    repo = defs.get_repository_def()
    schedules = {s.name for s in repo.schedule_defs}
    assert {
        "sample_daily_schedule",
        "live_market_fast_schedule",
        "live_research_hourly_schedule",
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
        "gold_alpha_events",
        "soda_data_quality",
        "data_issues",
        "markdown_reports",
    }
    assert expected.issubset(asset_keys)
