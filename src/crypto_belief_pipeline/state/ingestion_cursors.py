from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, ValidationError

from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.lake.s3 import get_s3_client

logger = logging.getLogger(__name__)

_NOT_FOUND_CODES = frozenset({"NoSuchKey", "NoSuchBucket", "404", "NotFound"})


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


def _is_not_found(exc: BaseException) -> bool:
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code")
        return code in _NOT_FOUND_CODES
    return False


def read_cursor(source: str, key: str, bucket: str | None = None) -> IngestionCursor | None:
    """Read a cursor from the lake.

    Returns ``None`` only when the cursor object does not yet exist (expected on
    first run for a given source/key). Other failures (auth, transport, parse,
    schema) propagate so they remain visible.
    """

    s = get_settings()
    b = bucket or s.s3_bucket
    obj_key = cursor_object_key(source, key)
    prefix = f"{s.s3_prefix.strip('/')}/" if s.s3_prefix else ""
    full_key = prefix + obj_key
    client = get_s3_client(settings=s)
    try:
        obj = client.get_object(Bucket=b, Key=full_key)
    except ClientError as e:
        if _is_not_found(e):
            return None
        logger.error(
            "cursor read failed: bucket=%s key=%s error=%s",
            b,
            full_key,
            e.response.get("Error", {}).get("Code"),
        )
        raise

    body_bytes = obj["Body"].read()
    try:
        data = json.loads(body_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(f"cursor object is not valid JSON: bucket={b!r} key={full_key!r}") from e

    try:
        return IngestionCursor.model_validate(data)
    except ValidationError as e:
        raise ValueError(f"cursor schema mismatch: bucket={b!r} key={full_key!r}") from e


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
