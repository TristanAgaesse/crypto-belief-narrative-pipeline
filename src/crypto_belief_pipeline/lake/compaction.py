from __future__ import annotations

import io
from dataclasses import dataclass

import polars as pl

from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.lake.keys import full_s3_key
from crypto_belief_pipeline.lake.paths import microbatch_dir, partition_path
from crypto_belief_pipeline.lake.s3 import get_s3_client
from crypto_belief_pipeline.lake.write import write_parquet_df


@dataclass(frozen=True)
class CompactionResult:
    input_files: int
    output_key: str


def _list_keys(prefix: str) -> list[str]:
    s = get_settings()
    client = get_s3_client(settings=s)
    full_prefix = full_s3_key(prefix).rstrip("/") + "/"

    out: list[str] = []
    token = None
    while True:
        kwargs = {"Bucket": s.s3_bucket, "Prefix": full_prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for it in resp.get("Contents") or []:
            k = it.get("Key")
            if isinstance(k, str):
                # Return de-prefixed keys (callers use lake-style relative keys)
                if s.s3_prefix:
                    rel = k[len(full_s3_key("")) :].lstrip("/")  # remove prefix + leading slash
                    out.append(rel)
                else:
                    out.append(k)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            continue
        break
    return sorted(set(out))


def _read_parquet_keys(keys: list[str]) -> pl.DataFrame:
    if not keys:
        return pl.DataFrame()
    s = get_settings()
    client = get_s3_client(settings=s)
    dfs: list[pl.DataFrame] = []
    for k in keys:
        obj = client.get_object(Bucket=s.s3_bucket, Key=full_s3_key(k))
        dfs.append(pl.read_parquet(io.BytesIO(obj["Body"].read())))
    return pl.concat(dfs, how="vertical_relaxed") if len(dfs) > 1 else dfs[0]


def compact_hourly_microbatches(
    *,
    layer: str,
    dataset: str,
    run_date: str,
    hour: int,
) -> CompactionResult | None:
    """Compact `{layer}/{dataset}/date=.../hour=HH/batch_id=...*.parquet` to one hourly file."""

    src_prefix = microbatch_dir(layer=layer, dataset=dataset, dt=run_date, hour=hour)
    keys = [k for k in _list_keys(src_prefix) if k.endswith(".parquet") and "/batch_id=" in k]
    if not keys:
        return None

    df = _read_parquet_keys(keys)
    out_dataset = f"{dataset}_compacted"
    out_prefix = microbatch_dir(layer=layer, dataset=out_dataset, dt=run_date, hour=hour)
    out_key = f"{out_prefix}/data.parquet"
    write_parquet_df(df, out_key)
    return CompactionResult(input_files=len(keys), output_key=out_key)


def compact_daily_from_hourly(
    *,
    layer: str,
    dataset: str,
    run_date: str,
) -> CompactionResult | None:
    """Compact hourly `.../{dataset}_compacted/date=.../hour=HH/data.parquet` to daily `data.parquet`."""

    hourly_dataset = f"{dataset}_compacted"
    prefix = partition_path(layer, hourly_dataset, run_date)
    keys = [k for k in _list_keys(prefix) if k.endswith("/data.parquet") and "/hour=" in k]
    if not keys:
        return None

    df = _read_parquet_keys(keys)
    out_key = f"{partition_path(layer, hourly_dataset, run_date)}/data.parquet"
    write_parquet_df(df, out_key)
    return CompactionResult(input_files=len(keys), output_key=out_key)

