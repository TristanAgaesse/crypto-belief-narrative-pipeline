from datetime import date, datetime

import pytest

from crypto_belief_pipeline.lake.paths import partition_path


def test_partition_path_accepts_date() -> None:
    assert (
        partition_path("raw", "provider=polymarket", date(2026, 5, 6))
        == "raw/provider=polymarket/date=2026-05-06"
    )


def test_partition_path_accepts_datetime() -> None:
    assert (
        partition_path("raw", "provider=polymarket", datetime(2026, 5, 6, 12, 0, 0))
        == "raw/provider=polymarket/date=2026-05-06"
    )


def test_partition_path_accepts_string() -> None:
    assert (
        partition_path("raw", "provider=polymarket", "2026-05-06")
        == "raw/provider=polymarket/date=2026-05-06"
    )


def test_partition_path_rejects_invalid_layer() -> None:
    with pytest.raises(ValueError):
        partition_path("oops", "provider=polymarket", "2026-05-06")
