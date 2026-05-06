import io
import json
from typing import Any

import polars as pl

from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.lake.s3 import get_s3_client

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None


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
    client.put_object(Bucket=b, Key=key, Body=body, ContentType="application/x-ndjson")


def write_parquet_df(df: Any, key: str, bucket: str | None = None) -> None:
    settings = get_settings()
    b = bucket or settings.s3_bucket
    pl_df = _to_polars(df)
    buf = io.BytesIO()
    pl_df.write_parquet(buf)
    client = get_s3_client(settings=settings)
    client.put_object(
        Bucket=b,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
