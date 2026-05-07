from __future__ import annotations

import pytest

import crypto_belief_pipeline.io_guardrails as ig


class _LiveSettings:
    s3_bucket = "live-bucket"


def _patch_config(
    monkeypatch,
    *,
    enabled: bool,
    sample_bucket: str | None,
    live_bucket: str = "live-bucket",
) -> None:
    class _Sample:
        sample_enabled = enabled
        sample_lake_bucket = sample_bucket

    class _Runtime:
        sample = _Sample()

    class _Settings:
        s3_bucket = live_bucket

    monkeypatch.setattr(ig, "get_runtime_config", lambda: _Runtime())
    monkeypatch.setattr(ig, "get_settings", lambda: _Settings())


def test_resolve_sample_bucket_returns_configured_bucket(monkeypatch) -> None:
    _patch_config(monkeypatch, enabled=True, sample_bucket="sample-bucket")
    assert ig.resolve_sample_bucket() == "sample-bucket"


def test_resolve_sample_bucket_strips_whitespace(monkeypatch) -> None:
    _patch_config(monkeypatch, enabled=True, sample_bucket="  sample-bucket  ")
    assert ig.resolve_sample_bucket() == "sample-bucket"


def test_resolve_sample_bucket_rejects_disabled(monkeypatch) -> None:
    _patch_config(monkeypatch, enabled=False, sample_bucket="sample-bucket")
    with pytest.raises(ig.SampleConfigError, match="Sample I/O is disabled"):
        ig.resolve_sample_bucket()


def test_resolve_sample_bucket_rejects_missing_bucket(monkeypatch) -> None:
    _patch_config(monkeypatch, enabled=True, sample_bucket=None)
    with pytest.raises(ig.SampleConfigError, match="sample_lake_bucket"):
        ig.resolve_sample_bucket()


def test_resolve_sample_bucket_rejects_empty_bucket(monkeypatch) -> None:
    _patch_config(monkeypatch, enabled=True, sample_bucket="   ")
    with pytest.raises(ig.SampleConfigError, match="sample_lake_bucket"):
        ig.resolve_sample_bucket()


def test_resolve_sample_bucket_rejects_equal_to_live_bucket(monkeypatch) -> None:
    """Critical safety: sample bucket equal to live bucket must be rejected.

    Without this check, sample writes would land directly in production data.
    """

    _patch_config(monkeypatch, enabled=True, sample_bucket="live-bucket", live_bucket="live-bucket")
    with pytest.raises(ig.SampleConfigError, match="must differ from the live S3_BUCKET"):
        ig.resolve_sample_bucket()


def test_sample_config_error_is_value_error_subclass() -> None:
    """``except ValueError`` must keep working for callers that don't import the new type."""

    assert issubclass(ig.SampleConfigError, ValueError)
