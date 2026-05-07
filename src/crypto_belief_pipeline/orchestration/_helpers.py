"""Shared helpers + partitions definition for orchestration assets.

This module lives separately from the asset definitions so that:

* The :class:`DailyPartitionsDefinition` is created exactly once and shared,
* Helpers can be unit-tested without importing the full Dagster asset graph,
* Asset modules can grow independently without bloating a single 700-line file.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from crypto_belief_pipeline.io_guardrails import resolve_sample_bucket
from crypto_belief_pipeline.lake.read import LakeKeyNotFound, read_jsonl_records
from dagster import DailyPartitionsDefinition

partitions_def = DailyPartitionsDefinition(start_date="2026-05-06")


def _partition_bounds_utc(run_date: date) -> tuple[datetime, datetime]:
    start = datetime(run_date.year, run_date.month, run_date.day, tzinfo=UTC)
    return start, start + timedelta(days=1)


def _partition_tick_now_utc(run_date: date) -> datetime:
    """Return a deterministic 'now' that never leaves the partition's UTC day.

    This prevents historical partition runs (backfills) from writing microbatches under today's
    `date=` directory.
    """

    part_start, part_end = _partition_bounds_utc(run_date)
    now = datetime.now(UTC)

    if now >= part_end:
        now = part_end - timedelta(minutes=1)
    if now < part_start:
        now = part_start

    return now.replace(second=0, microsecond=0)


def _clamp_window_to_partition(
    window_start: datetime, window_end: datetime, run_date: date
) -> tuple[datetime, datetime]:
    part_start, part_end = _partition_bounds_utc(run_date)
    st = max(window_start, part_start)
    et = min(window_end, part_end)
    if st >= et:
        st = max(part_start, et - timedelta(minutes=1))
    return st, et


def _k(prefix: str, name: str) -> str:
    return f"{prefix}/{name}"


def _read_yaml_mapping(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data if isinstance(data, dict) else {}


def _dup_metrics(df, subset: list[str]) -> dict[str, Any]:
    """Compute cheap duplicate metrics for observability (no mutation)."""
    try:
        total = int(getattr(df, "height", 0))
        if total == 0:
            return {
                "rows_total": 0,
                "rows_distinct_on_key": 0,
                "duplicate_rows": 0,
                "duplicate_rate": 0.0,
            }
        distinct = int(df.unique(subset=subset).height)
        dup = max(total - distinct, 0)
        return {
            "rows_total": total,
            "rows_distinct_on_key": distinct,
            "duplicate_rows": dup,
            "duplicate_rate": float(dup / total) if total else 0.0,
            "dedup_keys": subset,
        }
    except Exception:
        return {"rows_total": int(getattr(df, "height", 0)), "dedup_keys": subset}


def _with_lineage(df: pl.DataFrame, upstream: dict[str, str]) -> pl.DataFrame:
    return df.with_columns(
        pl.lit(upstream.get("source_batch_id") or "").alias("source_batch_id"),
        pl.lit(upstream.get("source_window_start") or "").alias("source_window_start"),
        pl.lit(upstream.get("source_window_end") or "").alias("source_window_end"),
    )


def _sample_guardrails() -> str:
    """Return the dedicated sample bucket and validate sample I/O is enabled.

    Thin alias around :func:`resolve_sample_bucket` so legacy imports of this
    helper continue to work. The shared validator enforces enable + non-empty
    + distinct-from-live-bucket policy in one place.
    """

    return resolve_sample_bucket()


def _safe_read_jsonl(key: str) -> list[dict]:
    """Read raw JSONL, treating missing-but-expected keys as empty.

    Missing keys are a legitimate empty-batch signal in this pipeline (e.g.
    GDELT can return zero rows for sparse narratives). All other failures
    (auth, transport, parse) propagate so the run surfaces them clearly
    instead of producing silently empty bronze.
    """
    try:
        return read_jsonl_records(key)
    except LakeKeyNotFound:
        return []


__all__ = [
    "_clamp_window_to_partition",
    "_dup_metrics",
    "_k",
    "_partition_bounds_utc",
    "_partition_tick_now_utc",
    "_read_yaml_mapping",
    "_safe_read_jsonl",
    "_sample_guardrails",
    "_with_lineage",
    "partitions_def",
]
