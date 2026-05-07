from __future__ import annotations

import io
import json
from typing import Any

import polars as pl
from botocore.exceptions import ClientError

from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.lake.keys import full_s3_key
from crypto_belief_pipeline.lake.s3 import get_s3_client

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None  # type: ignore[assignment]


_NOT_FOUND_CODES = frozenset({"NoSuchKey", "NoSuchBucket", "404", "NotFound"})


def _is_not_found(exc: BaseException) -> bool:
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code")
        return code in _NOT_FOUND_CODES
    return False


def _to_polars(df: Any) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame):
        return df
    if pd is not None and isinstance(df, pd.DataFrame):
        return pl.from_pandas(df)
    raise TypeError("Expected a polars.DataFrame or pandas.DataFrame")


def write_jsonl_records(records: list[dict], key: str, bucket: str | None = None) -> None:
    settings = get_settings()
    b = bucket or settings.s3_bucket
    body = ("\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n").encode("utf-8")
    client = get_s3_client(settings=settings)
    full_key = full_s3_key(key)
    try:
        client.put_object(Bucket=b, Key=full_key, Body=body, ContentType="application/x-ndjson")
    except ClientError as e:
        if _is_not_found(e):
            # PUT on a missing bucket is a real failure (not "expected absence").
            raise RuntimeError(
                f"S3 put_object failed (target missing): bucket={b!r} key={full_key!r}"
            ) from e
        raise


def write_parquet_df(df: Any, key: str, bucket: str | None = None) -> None:
    settings = get_settings()
    b = bucket or settings.s3_bucket
    pl_df = _to_polars(df)
    buf = io.BytesIO()
    pl_df.write_parquet(buf)
    client = get_s3_client(settings=settings)
    full_key = full_s3_key(key)
    try:
        client.put_object(
            Bucket=b,
            Key=full_key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
    except ClientError as e:
        if _is_not_found(e):
            raise RuntimeError(
                f"S3 put_object failed (target missing): bucket={b!r} key={full_key!r}"
            ) from e
        raise
