from __future__ import annotations

import io
import json

import polars as pl

from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.lake.keys import full_s3_key
from crypto_belief_pipeline.lake.s3 import get_s3_client


def read_jsonl_records(key: str, bucket: str | None = None) -> list[dict]:
    settings = get_settings()
    b = bucket or settings.s3_bucket
    client = get_s3_client(settings=settings)
    obj = client.get_object(Bucket=b, Key=full_s3_key(key))
    text = obj["Body"].read().decode("utf-8")
    out: list[dict] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        out.append(json.loads(line))
    return out


def read_parquet_df(key: str, bucket: str | None = None) -> pl.DataFrame:
    settings = get_settings()
    b = bucket or settings.s3_bucket
    client = get_s3_client(settings=settings)
    obj = client.get_object(Bucket=b, Key=full_s3_key(key))
    return pl.read_parquet(io.BytesIO(obj["Body"].read()))
