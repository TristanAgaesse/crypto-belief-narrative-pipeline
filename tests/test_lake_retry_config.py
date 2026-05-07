from __future__ import annotations

import boto3
from botocore.config import Config

import crypto_belief_pipeline.lake.s3 as s3_mod
from crypto_belief_pipeline.config import Settings


def _settings() -> Settings:
    return Settings(
        aws_endpoint_url="http://localhost:9000",
        aws_access_key_id="x",
        aws_secret_access_key="y",
        aws_region="us-east-1",
        s3_bucket="b",
    )


def test_get_s3_client_configures_retry_policy(monkeypatch) -> None:
    """boto3 client must be configured with a transient-error retry policy.

    Without retries, transient MinIO/S3 hiccups (throttling, connection resets)
    cause whole-pipeline failures. We rely on botocore's standard retry mode for
    this rather than manually wrapping each call site.
    """

    captured: dict[str, object] = {}

    real_client = boto3.client

    def spy(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return real_client(*args, **kwargs)

    monkeypatch.setattr(boto3, "client", spy)

    s3_mod.get_s3_client(settings=_settings())

    assert captured["args"] == ("s3",)
    cfg = captured["kwargs"].get("config")
    assert isinstance(cfg, Config)
    retries = getattr(cfg, "retries", None) or {}
    assert isinstance(retries, dict)
    # botocore normalizes max_attempts (initial + retries) to total_max_attempts;
    # accept either form for forward-compat across botocore versions.
    attempts = retries.get("total_max_attempts") or retries.get("max_attempts") or 0
    assert attempts >= 3, f"expected >=3 attempts, got {attempts!r} (full retries={retries!r})"
    assert retries.get("mode") in {"standard", "adaptive", "legacy"}
