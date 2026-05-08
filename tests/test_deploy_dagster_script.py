from __future__ import annotations

from pathlib import Path


def test_deploy_validate_uses_partition_specific_keys() -> None:
    script = Path("scripts/deploy_dagster.sh").read_text(encoding="utf-8")

    assert "_validation_minute_partition()" in script
    assert "_validation_hourly_partition()" in script
    assert (
        "raw_staging__binance__1m_job raw_staging__polymarket__5m_job raw_staging__gdelt__1h_job"
    ) in script
    assert (
        "raw_to_silver__binance__1m_job "
        "raw_to_silver__polymarket__5m_job "
        "raw_to_silver__gdelt__1h_job"
    ) in script
    assert '_validate_ingestion_job "$job" "$MINUTE_PARTITION"' in script
    assert '_validate_ingestion_job "$job" "$HOURLY_PARTITION"' in script
