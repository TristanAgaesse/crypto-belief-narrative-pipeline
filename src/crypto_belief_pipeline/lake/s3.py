from __future__ import annotations

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from crypto_belief_pipeline.config import Settings, get_settings

# botocore "standard" retry mode retries on throttling and transient 5xx
# errors with exponential backoff. 5 attempts gives ~30s of total tolerance,
# which is enough for short network blips without masking real failures.
_DEFAULT_RETRY_CONFIG = {"max_attempts": 5, "mode": "standard"}


def _build_boto_config() -> Config:
    return Config(
        s3={"addressing_style": "path"},
        retries=_DEFAULT_RETRY_CONFIG,
    )


def get_s3_client(settings: Settings | None = None):
    s = settings or get_settings()
    if not s.aws_endpoint_url:
        raise ValueError("aws_endpoint_url is required (this project defaults to MinIO, not AWS).")

    return boto3.client(
        "s3",
        endpoint_url=s.aws_endpoint_url,
        aws_access_key_id=s.aws_access_key_id,
        aws_secret_access_key=s.aws_secret_access_key,
        region_name=s.aws_region,
        config=_build_boto_config(),
    )


def ensure_bucket_exists(bucket: str, settings: Settings | None = None) -> None:
    client = get_s3_client(settings=settings)
    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            # MinIO may also return 403 for non-existing buckets depending on policy;
            # treat those as "create" only if the error isn't something else.
            if code not in {"403", "Forbidden"}:
                raise

    # For MinIO, CreateBucketConfiguration is optional and sometimes rejected;
    # keep it simple.
    client.create_bucket(Bucket=bucket)
