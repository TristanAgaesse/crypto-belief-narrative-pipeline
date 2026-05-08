from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import crypto_belief_pipeline.lake.read as lr


def test_read_jsonl_strict_raises_on_bad_json(monkeypatch) -> None:
    body = b'{"ok":1}\nnot-json\n{"ok":2}\n'
    mock_client = MagicMock()
    mock_client.get_object.return_value = {"Body": MagicMock(read=lambda: body)}
    monkeypatch.setattr(lr, "get_s3_client", lambda settings: mock_client)
    monkeypatch.setattr(lr, "full_s3_key", lambda k: k)
    monkeypatch.setattr(lr, "get_settings", lambda: MagicMock(s3_bucket="b"))

    with pytest.raises(lr.MalformedJsonlLineError) as ei:
        lr.read_jsonl_records("raw/x.jsonl")
    assert ei.value.line_no == 2
    assert "raw/x.jsonl" in str(ei.value)


def test_read_jsonl_skip_returns_valid_rows_and_stats(monkeypatch) -> None:
    body = b'{"a": 1}\ntruncated\n[1,2]\n{"b": 2}\n'
    mock_client = MagicMock()
    mock_client.get_object.return_value = {"Body": MagicMock(read=lambda: body)}
    monkeypatch.setattr(lr, "get_s3_client", lambda settings: mock_client)
    monkeypatch.setattr(lr, "full_s3_key", lambda k: k)
    monkeypatch.setattr(lr, "get_settings", lambda: MagicMock(s3_bucket="b"))

    stats: dict = {}
    rows = lr.read_jsonl_records(
        "k.jsonl",
        on_invalid_line="skip",
        invalid_line_stats=stats,
    )
    assert rows == [{"a": 1}, {"b": 2}]
    assert stats["skipped_lines"] == 2
    assert "first_error" in stats
