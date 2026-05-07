from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from crypto_belief_pipeline.config import Settings
from crypto_belief_pipeline.lake.s3 import ensure_bucket_exists, get_s3_client


def test_get_s3_client_requires_endpoint_url() -> None:
    s = Settings(
        aws_endpoint_url="",
        aws_access_key_id="x",
        aws_secret_access_key="y",
        aws_region="us-east-1",
        s3_bucket="b",
    )
    with pytest.raises(ValueError, match="aws_endpoint_url is required"):
        get_s3_client(settings=s)


def test_ensure_bucket_exists_creates_on_404(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    class _Client:
        def head_bucket(self, Bucket: str) -> None:  # noqa: N803
            calls.append(("head_bucket", Bucket))
            raise ClientError({"Error": {"Code": "404", "Message": "Not found"}}, "HeadBucket")

        def create_bucket(self, Bucket: str) -> None:  # noqa: N803
            calls.append(("create_bucket", Bucket))

    monkeypatch.setattr(
        "crypto_belief_pipeline.lake.s3.get_s3_client",
        lambda settings=None: _Client(),
    )
    ensure_bucket_exists("my-bucket")
    assert calls == [("head_bucket", "my-bucket"), ("create_bucket", "my-bucket")]


def test_ensure_bucket_exists_raises_on_unexpected_error(monkeypatch) -> None:
    class _Client:
        def head_bucket(self, Bucket: str) -> None:  # noqa: N803
            raise ClientError({"Error": {"Code": "500", "Message": "Nope"}}, "HeadBucket")

    monkeypatch.setattr(
        "crypto_belief_pipeline.lake.s3.get_s3_client",
        lambda settings=None: _Client(),
    )
    with pytest.raises(ClientError):
        ensure_bucket_exists("b")
