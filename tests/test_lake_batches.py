from __future__ import annotations

from datetime import UTC, datetime

import pytest

from crypto_belief_pipeline.lake.batches import generate_batch_id, split_batch_parts


def test_generate_batch_id_is_utc_and_sortable() -> None:
    t1 = datetime(2026, 5, 6, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 5, 6, 10, 0, 1, tzinfo=UTC)
    b1 = generate_batch_id(t1)
    b2 = generate_batch_id(t2)
    assert b1 < b2
    assert b1.endswith("Z")


def test_split_batch_parts_round_trips() -> None:
    batch_id = "20260506T100000Z"
    parts = split_batch_parts(batch_id)
    assert parts.batch_id == batch_id
    assert parts.date == "2026-05-06"
    assert parts.hour == "10"


def test_split_batch_parts_rejects_invalid_format() -> None:
    with pytest.raises(ValueError):
        split_batch_parts("not-a-batch-id")
