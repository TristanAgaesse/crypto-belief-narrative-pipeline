from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.lake.s3 import get_s3_client


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class IngestionCursor(BaseModel):
    source: str
    key: str = Field(
        description="Source-specific key (e.g. symbol=BTCUSDT, market_id=..., narrative=bitcoin)."
    )
    last_successful_event_time: str | None = None
    last_successful_ingestion_time: str | None = None
    last_batch_id: str | None = None
    status: str = "unknown"
    extra: dict[str, Any] = Field(default_factory=dict)


def cursor_object_key(source: str, key: str) -> str:
    safe_key = key.replace("/", "_")
    return f"state/ingestion_cursors/source={source}/{safe_key}.json"


def read_cursor(source: str, key: str, bucket: str | None = None) -> IngestionCursor | None:
    s = get_settings()
    b = bucket or s.s3_bucket
    obj_key = cursor_object_key(source, key)
    client = get_s3_client(settings=s)
    try:
        prefix = f"{s.s3_prefix.strip('/')}/" if s.s3_prefix else ""
        obj = client.get_object(Bucket=b, Key=prefix + obj_key)
    except Exception:
        return None
    data = json.loads(obj["Body"].read().decode("utf-8"))
    return IngestionCursor.model_validate(data)


def write_cursor(cursor: IngestionCursor, bucket: str | None = None) -> str:
    s = get_settings()
    b = bucket or s.s3_bucket
    obj_key = cursor_object_key(cursor.source, cursor.key)
    ingested = cursor.last_successful_ingestion_time or _utc_now_iso()
    cursor = cursor.model_copy(update={"last_successful_ingestion_time": ingested})
    body = json.dumps(cursor.model_dump(), indent=2, sort_keys=True).encode("utf-8")
    client = get_s3_client(settings=s)
    full_key = (f"{s.s3_prefix.strip('/')}/" if s.s3_prefix else "") + obj_key
    client.put_object(Bucket=b, Key=full_key, Body=body, ContentType="application/json")
    return obj_key
