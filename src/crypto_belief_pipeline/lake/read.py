from __future__ import annotations

import io
import json
from typing import Any, Literal

import polars as pl
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.lake.keys import full_s3_key
from crypto_belief_pipeline.lake.s3 import get_s3_client
from crypto_belief_pipeline.lake.s3_errors import is_not_found


class LakeKeyNotFound(KeyError):
    """Raised when an expected lake object is missing.

    Callers can treat this distinctly from unexpected I/O failures, e.g. to
    surface optional-empty inputs while still allowing transport/permission
    errors to propagate as real failures.
    """


class MalformedJsonlLineError(ValueError):
    """Raised when a JSONL line fails to parse in strict mode."""

    def __init__(self, *, key: str, line_no: int, preview: str):
        self.key = key
        self.line_no = line_no
        self.preview = preview
        msg = f"Malformed JSONL at line {line_no} in {key!r}: {preview!r}"
        super().__init__(msg)


def _maybe_translate_not_found(exc: BaseException, *, key: str) -> BaseException:
    if is_not_found(exc):
        return LakeKeyNotFound(f"lake key not found: {key}")
    return exc


def read_jsonl_records(
    key: str,
    bucket: str | None = None,
    *,
    on_invalid_line: Literal["error", "skip"] = "error",
    invalid_line_stats: dict[str, Any] | None = None,
) -> list[dict]:
    """Read newline-delimited JSON objects from S3.

    ``on_invalid_line``:
    - ``error`` (default): raise :class:`MalformedJsonlLineError` with line number and preview.
    - ``skip``: omit bad lines; if ``invalid_line_stats`` is a dict, populate
      ``skipped_lines`` (int) and optionally ``first_error`` (str).
    """

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
    skipped = 0
    line_no = 0
    for line in text.splitlines():
        line_no += 1
        if not line.strip():
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError as e:
            preview = line.strip()[:200]
            if on_invalid_line == "error":
                raise MalformedJsonlLineError(key=key, line_no=line_no, preview=preview) from e
            skipped += 1
            if invalid_line_stats is not None and skipped == 1:
                invalid_line_stats.setdefault("first_error", str(e))
            continue
        if not isinstance(parsed, dict):
            if on_invalid_line == "error":
                raise MalformedJsonlLineError(
                    key=key,
                    line_no=line_no,
                    preview=str(parsed)[:200],
                )
            skipped += 1
            if invalid_line_stats is not None and skipped == 1:
                invalid_line_stats.setdefault("first_error", "line is not a JSON object")
            continue
        out.append(parsed)
    if invalid_line_stats is not None:
        invalid_line_stats["skipped_lines"] = invalid_line_stats.get("skipped_lines", 0) + skipped
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


def read_parquet_row_count(key: str, bucket: str | None = None) -> int:
    """Return row count from Parquet metadata without decoding columns."""

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
    raw = obj["Body"].read()
    return int(pq.ParquetFile(io.BytesIO(raw)).metadata.num_rows)


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
        # Even the compacted single-file layout can vary in timezone metadata
        # across partitions; keep datetime columns consistent before returning.
        return _normalize_datetime_timezones(read_parquet_df(canonical_key, bucket=bucket))
    except LakeKeyNotFound:
        pass

    keys = list_parquet_keys_under(partition_prefix, bucket=bucket)
    if not keys:
        return pl.DataFrame()

    frames: list[pl.DataFrame] = []
    for k in keys:
        try:
            frames.append(_normalize_datetime_timezones(read_parquet_df(k, bucket=bucket)))
        except LakeKeyNotFound:
            # Concurrent compaction can remove a shard between list and read; skip it.
            continue

    if not frames:
        return pl.DataFrame()
    if len(frames) == 1:
        return frames[0]
    # `diagonal_relaxed` still requires identical dtypes for columns with the
    # same name; ensure datetime tz metadata is consistent across shards.
    return _normalize_datetime_timezones(pl.concat(frames, how="diagonal_relaxed"))


def _normalize_datetime_timezones(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize datetime columns to a consistent UTC time zone.

    Parquet shards can contain the same logical column as tz-naive in some files
    and tz-aware in others (e.g. `datetime[μs]` vs `datetime[μs, UTC]`). Polars
    requires identical dtypes to concatenate.

    Policy:
    - tz-naive datetimes are assumed to already be UTC and get `UTC` attached via
      `replace_time_zone` (no wall-time shift).
    - tz-aware datetimes are converted to `UTC`.
    """

    # Even empty shards can carry schema we need to normalize before concat.
    # Only skip when there are no columns at all.
    if df.width == 0:
        return df

    exprs: list[pl.Expr] = []
    for name, dtype in df.schema.items():
        if isinstance(dtype, pl.Datetime):
            tz = getattr(dtype, "time_zone", None)
            if tz is None:
                exprs.append(pl.col(name).dt.replace_time_zone("UTC"))
            else:
                exprs.append(pl.col(name).dt.convert_time_zone("UTC"))
    if not exprs:
        return df
    # IMPORTANT: pass expressions as positional args. Passing a list as a single
    # positional argument is treated as a literal and results in a no-op.
    return df.with_columns(*exprs)


__all__ = [
    "LakeKeyNotFound",
    "MalformedJsonlLineError",
    "list_jsonl_keys_under",
    "list_parquet_keys_under",
    "read_jsonl_records",
    "read_parquet_df",
    "read_parquet_partition_df",
    "read_parquet_row_count",
]
