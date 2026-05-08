from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from botocore.exceptions import ClientError
from pydantic import BaseModel, Field

from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.lake.s3 import get_s3_client

_NOT_FOUND_CODES = frozenset({"NoSuchKey", "NoSuchBucket", "404", "NotFound"})


def _is_not_found(exc: BaseException) -> bool:
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code")
        return code in _NOT_FOUND_CODES
    return False


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class ProcessingWatermark(BaseModel):
    consumer_asset: str
    source: str
    partition_key: str
    partition_start: str
    partition_end: str
    last_processed_batch_id: str | None = None
    last_processed_event_time: str | None = None
    last_processed_ingestion_time: str | None = None
    processed_input_count: int = 0
    processed_input_keys_sha1: str = ""
    status: str = Field(default="ok")
    failed_input_keys: list[str] = Field(default_factory=list)
    updated_at: str = Field(default_factory=_utc_now_iso)
    code_version: str = "unknown"

    @staticmethod
    def hash_input_keys(keys: list[str]) -> str:
        joined = "\n".join(sorted(keys))
        return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def watermark_object_key(consumer_asset: str, source: str, partition_key: str) -> str:
    safe_partition = partition_key.replace("/", "_")
    return (
        "state/processing_watermarks/"
        f"consumer={consumer_asset}/source={source}/partition={safe_partition}.json"
    )


def read_processing_watermark(
    consumer_asset: str,
    source: str,
    partition_key: str,
    bucket: str | None = None,
) -> ProcessingWatermark | None:
    s = get_settings()
    b = bucket or s.s3_bucket
    obj_key = watermark_object_key(consumer_asset, source, partition_key)
    full_key = (f"{s.s3_prefix.strip('/')}/" if s.s3_prefix else "") + obj_key
    client = get_s3_client(settings=s)
    try:
        obj = client.get_object(Bucket=b, Key=full_key)
    except ClientError as e:
        if _is_not_found(e):
            return None
        raise
    data = json.loads(obj["Body"].read().decode("utf-8"))
    return ProcessingWatermark.model_validate(data)


def write_processing_watermark(
    watermark: ProcessingWatermark,
    bucket: str | None = None,
) -> str:
    s = get_settings()
    b = bucket or s.s3_bucket
    obj_key = watermark_object_key(
        watermark.consumer_asset, watermark.source, watermark.partition_key
    )
    watermark = watermark.model_copy(update={"updated_at": _utc_now_iso()})
    body = json.dumps(watermark.model_dump(), indent=2, sort_keys=True).encode("utf-8")
    client = get_s3_client(settings=s)
    full_key = (f"{s.s3_prefix.strip('/')}/" if s.s3_prefix else "") + obj_key
    client.put_object(Bucket=b, Key=full_key, Body=body, ContentType="application/json")
    return obj_key


def build_watermark(
    *,
    consumer_asset: str,
    source: str,
    partition_key: str,
    partition_start: datetime,
    partition_end: datetime,
    processed_input_keys: list[str],
    code_version: str = "unknown",
    last_processed_batch_id: str | None = None,
    last_processed_event_time: str | None = None,
    last_processed_ingestion_time: str | None = None,
    status: str = "ok",
    failed_input_keys: list[str] | None = None,
) -> ProcessingWatermark:
    return ProcessingWatermark(
        consumer_asset=consumer_asset,
        source=source,
        partition_key=partition_key,
        partition_start=partition_start.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        partition_end=partition_end.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        last_processed_batch_id=last_processed_batch_id,
        last_processed_event_time=last_processed_event_time,
        last_processed_ingestion_time=last_processed_ingestion_time,
        processed_input_count=len(processed_input_keys),
        processed_input_keys_sha1=ProcessingWatermark.hash_input_keys(processed_input_keys),
        status=status,
        failed_input_keys=failed_input_keys or [],
        code_version=code_version,
    )


__all__ = [
    "ProcessingWatermark",
    "build_watermark",
    "read_processing_watermark",
    "watermark_object_key",
    "write_processing_watermark",
]
