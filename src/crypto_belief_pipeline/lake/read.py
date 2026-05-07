from __future__ import annotations

import io
import json

import polars as pl
from botocore.exceptions import ClientError

from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.lake.keys import full_s3_key
from crypto_belief_pipeline.lake.s3 import get_s3_client

_NOT_FOUND_CODES = frozenset({"NoSuchKey", "NoSuchBucket", "404", "NotFound"})


class LakeKeyNotFound(KeyError):
    """Raised when an expected lake object is missing.

    Callers can treat this distinctly from unexpected I/O failures, e.g. to
    surface optional-empty inputs while still allowing transport/permission
    errors to propagate as real failures.
    """


def _is_not_found(exc: BaseException) -> bool:
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code")
        return code in _NOT_FOUND_CODES
    return False


def _maybe_translate_not_found(exc: BaseException, *, key: str) -> BaseException:
    if _is_not_found(exc):
        return LakeKeyNotFound(f"lake key not found: {key}")
    return exc


def read_jsonl_records(key: str, bucket: str | None = None) -> list[dict]:
    settings = get_settings()
    b = bucket or settings.s3_bucket
    client = get_s3_client(settings=settings)
    full_key = full_s3_key(key)
    try:
        obj = client.get_object(Bucket=b, Key=full_key)
    except ClientError as e:
        translated = _maybe_translate_not_found(e, key=full_key)
        if isinstance(translated, LakeKeyNotFound):
            raise translated from e
        raise
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
    full_key = full_s3_key(key)
    try:
        obj = client.get_object(Bucket=b, Key=full_key)
    except ClientError as e:
        translated = _maybe_translate_not_found(e, key=full_key)
        if isinstance(translated, LakeKeyNotFound):
            raise translated from e
        raise
    return pl.read_parquet(io.BytesIO(obj["Body"].read()))


def list_parquet_keys_under(partition_prefix: str, bucket: str | None = None) -> list[str]:
    """List lake-relative keys for parquet objects under a partition prefix.

    Returned keys do NOT include any configured ``s3_prefix`` and are suitable
    for passing back to :func:`read_parquet_df`.
    """

    settings = get_settings()
    b = bucket or settings.s3_bucket
    client = get_s3_client(settings=settings)

    full_prefix = full_s3_key(partition_prefix.rstrip("/")) + "/"
    s3_prefix_part = full_s3_key("")
    if s3_prefix_part and not s3_prefix_part.endswith("/"):
        s3_prefix_part = s3_prefix_part + "/"

    out: list[str] = []
    token: str | None = None
    while True:
        kwargs: dict[str, object] = {"Bucket": b, "Prefix": full_prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for item in resp.get("Contents") or []:
            full_key = item.get("Key")
            if not isinstance(full_key, str) or not full_key.endswith(".parquet"):
                continue
            rel_key = full_key
            if s3_prefix_part and rel_key.startswith(s3_prefix_part):
                rel_key = rel_key[len(s3_prefix_part) :]
            out.append(rel_key)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
        if not token:
            break

    return sorted(set(out))


def list_jsonl_keys_under(partition_prefix: str, bucket: str | None = None) -> list[str]:
    """List lake-relative keys for ``*.jsonl`` objects under a partition prefix."""

    settings = get_settings()
    b = bucket or settings.s3_bucket
    client = get_s3_client(settings=settings)

    full_prefix = full_s3_key(partition_prefix.rstrip("/")) + "/"
    s3_prefix_part = full_s3_key("")
    if s3_prefix_part and not s3_prefix_part.endswith("/"):
        s3_prefix_part = s3_prefix_part + "/"

    out: list[str] = []
    token: str | None = None
    while True:
        kwargs: dict[str, object] = {"Bucket": b, "Prefix": full_prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for item in resp.get("Contents") or []:
            full_key = item.get("Key")
            if not isinstance(full_key, str) or not full_key.endswith(".jsonl"):
                continue
            rel_key = full_key
            if s3_prefix_part and rel_key.startswith(s3_prefix_part):
                rel_key = rel_key[len(s3_prefix_part) :]
            out.append(rel_key)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
        if not token:
            break

    return sorted(set(out))


def read_parquet_partition_df(partition_prefix: str, bucket: str | None = None) -> pl.DataFrame:
    """Read all parquet shards under a partition prefix into a single DataFrame.

    Resolution order:

    1. Prefer ``{partition_prefix}/data.parquet`` when present (compacted/CLI layout).
    2. Otherwise list and union all ``*.parquet`` objects under the prefix
       (Dagster microbatch layout: ``hour=HH/batch_id=*.parquet``).
    3. Return an empty DataFrame when no parquet objects exist.

    Unexpected (non-not-found) errors are not silently swallowed.
    """

    canonical_key = f"{partition_prefix.rstrip('/')}/data.parquet"
    try:
        return read_parquet_df(canonical_key, bucket=bucket)
    except LakeKeyNotFound:
        pass

    keys = list_parquet_keys_under(partition_prefix, bucket=bucket)
    if not keys:
        return pl.DataFrame()

    frames: list[pl.DataFrame] = []
    for k in keys:
        try:
            frames.append(read_parquet_df(k, bucket=bucket))
        except LakeKeyNotFound:
            # Concurrent compaction can remove a shard between list and read; skip it.
            continue

    if not frames:
        return pl.DataFrame()
    if len(frames) == 1:
        return frames[0]
    return pl.concat(frames, how="diagonal_relaxed")


__all__ = [
    "LakeKeyNotFound",
    "list_jsonl_keys_under",
    "list_parquet_keys_under",
    "read_jsonl_records",
    "read_parquet_df",
    "read_parquet_partition_df",
]
