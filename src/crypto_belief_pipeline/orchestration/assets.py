from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
import polars as pl
from dagster import (
    AssetDep,
    AssetKey,
    AssetOut,
    DailyPartitionsDefinition,
    MetadataValue,
    Output,
    asset,
    multi_asset,
)

from crypto_belief_pipeline.collectors.binance import collect_binance_raw
from crypto_belief_pipeline.collectors.gdelt import collect_gdelt_raw_window
from crypto_belief_pipeline.collectors.polymarket import collect_polymarket_raw
from crypto_belief_pipeline.dq.soda import run_soda_checks
from crypto_belief_pipeline.features.build_gold import build_gold_tables
from crypto_belief_pipeline.lake.batches import generate_batch_id, split_batch_parts
from crypto_belief_pipeline.lake.paths import microbatch_key, partition_path
from crypto_belief_pipeline.lake.read import read_jsonl_records, read_parquet_df
from crypto_belief_pipeline.lake.write import write_jsonl_records, write_parquet_df
from crypto_belief_pipeline.orchestration.resources import resolve_run_date
from crypto_belief_pipeline.quality.issues import detect_data_issues, write_data_issues_reports
from crypto_belief_pipeline.sample_data import load_sample_jsonl
from crypto_belief_pipeline.config import get_runtime_config
from crypto_belief_pipeline.orchestration.windows import compute_window
from crypto_belief_pipeline.state.ingestion_cursors import IngestionCursor, read_cursor, write_cursor
from crypto_belief_pipeline.transform.normalize_binance import (
    normalize_klines,
    to_crypto_candles_1m,
)
from crypto_belief_pipeline.transform.normalize_gdelt import normalize_timeline, to_narrative_counts
from crypto_belief_pipeline.transform.normalize_polymarket import (
    normalize_markets,
    normalize_price_snapshots,
    to_belief_price_snapshots,
)

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

    # If running after the partition day is over, anchor inside that day (last minute).
    if now >= part_end:
        now = part_end - timedelta(minutes=1)

    # If clocks are skewed or part_end is in the future, still keep within bounds.
    if now < part_start:
        now = part_start

    return now.replace(second=0, microsecond=0)


def _clamp_window_to_partition(window_start: datetime, window_end: datetime, run_date: date) -> tuple[datetime, datetime]:
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
            return {"rows_total": 0, "rows_distinct_on_key": 0, "duplicate_rows": 0, "duplicate_rate": 0.0}
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


def _sample_guardrails() -> tuple[str, str | None]:
    rt = get_runtime_config()
    if not rt.sample.sample_enabled:
        raise ValueError(
            "Sample I/O is disabled. Enable via config/runtime.yaml: sample.sample_enabled=true"
        )
    prefix = (rt.sample.sample_lake_prefix or "").strip("/")
    if not prefix:
        raise ValueError("sample.sample_lake_prefix must be non-empty when sample I/O is enabled")
    return prefix, rt.sample.sample_lake_bucket


def _sample_partition_path(layer: str, dataset: str, dt) -> str:
    prefix, _bucket = _sample_guardrails()
    return f"{prefix}/{partition_path(layer, dataset, dt)}"


@asset(
    partitions_def=partitions_def,
    description="Write deterministic sample raw JSONL inputs into the lake (raw/).",
)
def raw_sample_inputs(context) -> dict[str, str]:
    """Write sample JSONL into the lake under raw/ (partitioned by run_date)."""

    run_date = resolve_run_date(context.partition_key)
    pm_markets = load_sample_jsonl("polymarket_markets_sample.jsonl")
    pm_prices = load_sample_jsonl("polymarket_prices_sample.jsonl")
    bn_klines = load_sample_jsonl("binance_klines_sample.jsonl")
    gd_timeline = load_sample_jsonl("gdelt_timeline_sample.jsonl")

    sample_prefix, sample_bucket = _sample_guardrails()
    raw_pm = _sample_partition_path("raw", "provider=polymarket", run_date)
    raw_bn = _sample_partition_path("raw", "provider=binance", run_date)
    raw_gd = _sample_partition_path("raw", "provider=gdelt", run_date)

    written = {
        "raw_polymarket_markets": _k(raw_pm, "sample_markets.jsonl"),
        "raw_polymarket_prices": _k(raw_pm, "sample_prices.jsonl"),
        "raw_binance_klines": _k(raw_bn, "sample_klines.jsonl"),
        "raw_gdelt_timeline": _k(raw_gd, "sample_timeline.jsonl"),
    }
    write_jsonl_records(pm_markets, written["raw_polymarket_markets"], bucket=sample_bucket)
    write_jsonl_records(pm_prices, written["raw_polymarket_prices"], bucket=sample_bucket)
    write_jsonl_records(bn_klines, written["raw_binance_klines"], bucket=sample_bucket)
    write_jsonl_records(gd_timeline, written["raw_gdelt_timeline"], bucket=sample_bucket)

    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "sample_prefix": sample_prefix,
            "sample_bucket": sample_bucket or "",
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    description=(
        "Collect Polymarket live markets + prices into raw JSONL (raw/provider=polymarket)."
    ),
)
def raw_polymarket(context) -> dict[str, str]:
    """Collect Polymarket markets + prices and write raw JSONL to the lake."""

    run_date = resolve_run_date(context.partition_key)
    tick_now = _partition_tick_now_utc(run_date)
    batch_id = generate_batch_id(tick_now)
    parts = split_batch_parts(batch_id)
    rt = get_runtime_config()
    end_time = tick_now
    cursor = read_cursor("polymarket", "default")
    window = compute_window(
        now=end_time,
        cadence=timedelta(seconds=rt.cadence.polymarket_prices_seconds),
        overlap=timedelta(minutes=1),
        cursor=cursor,
    )
    start_time, end_time = _clamp_window_to_partition(window.start_time, window.end_time, run_date)
    cfg = _read_yaml_mapping("config/markets_keywords.yaml").get("polymarket") or {}
    keywords = cfg.get("keywords") or []
    active = bool(cfg.get("active", True))
    closed = bool(cfg.get("closed", False))

    markets, prices, pm_meta = collect_polymarket_raw(
        limit=int(cfg.get("limit", 100)),
        keywords=list(keywords) if isinstance(keywords, list) else [],
        start_time=start_time,
        end_time=end_time,
        active=active,
        closed=closed,
    )

    markets_key = microbatch_key(
        "raw", "provider=polymarket", parts.date, parts.hour, batch_id, "_markets.jsonl"
    )
    prices_key = microbatch_key(
        "raw", "provider=polymarket", parts.date, parts.hour, batch_id, "_prices.jsonl"
    )
    write_jsonl_records(markets, markets_key)
    write_jsonl_records(prices, prices_key)
    write_cursor(
        IngestionCursor(
            source="polymarket",
            key="default",
            last_successful_event_time=pm_meta.get("max_event_time"),
            last_batch_id=batch_id,
            status="ok",
        )
    )

    written = {
        "raw_polymarket_markets": markets_key,
        "raw_polymarket_prices": prices_key,
        "source_batch_id": batch_id,
        "source_window_start": str(pm_meta.get("start_time") or ""),
        "source_window_end": str(pm_meta.get("end_time") or ""),
    }
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "markets_rows": len(markets),
            "prices_rows": len(prices),
            "batch_id": batch_id,
            "source_window": MetadataValue.json(
                {"start_time": pm_meta.get("start_time"), "end_time": pm_meta.get("end_time")}
            ),
            "missing_updatedAt_count": int(pm_meta.get("missing_updatedAt_count") or 0),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    description="Collect Binance live klines into raw JSONL (raw/provider=binance).",
)
def raw_binance(context) -> dict[str, str]:
    """Collect recent Binance klines and write raw JSONL to the lake."""

    run_date = resolve_run_date(context.partition_key)
    tick_now = _partition_tick_now_utc(run_date)
    batch_id = generate_batch_id(tick_now)
    parts = split_batch_parts(batch_id)
    rt = get_runtime_config()
    end_time = tick_now
    cursor = read_cursor("binance", "symbols=BTCUSDT,ETHUSDT,SOLUSDT")
    window = compute_window(
        now=end_time,
        cadence=timedelta(seconds=rt.cadence.binance_raw_seconds),
        overlap=timedelta(minutes=1),
        cursor=cursor,
    )
    start_time, end_time = _clamp_window_to_partition(window.start_time, window.end_time, run_date)
    klines, bn_meta = collect_binance_raw(limit=1000, start_time=start_time, end_time=end_time)
    key = microbatch_key("raw", "provider=binance", parts.date, parts.hour, batch_id, ".jsonl")
    write_jsonl_records(klines, key)
    write_cursor(
        IngestionCursor(
            source="binance",
            key="symbols=BTCUSDT,ETHUSDT,SOLUSDT",
            last_successful_event_time=bn_meta.get("max_event_time"),
            last_batch_id=batch_id,
            status="ok",
        )
    )

    written = {
        "raw_binance_klines": key,
        "source_batch_id": batch_id,
        "source_window_start": str(bn_meta.get("start_time") or ""),
        "source_window_end": str(bn_meta.get("end_time") or ""),
    }
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": len(klines),
            "batch_id": batch_id,
            "source_window": MetadataValue.json(
                {"start_time": bn_meta.get("start_time"), "end_time": bn_meta.get("end_time")}
            ),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    description=(
        "Collect GDELT TimelineVol into raw JSONL (raw/provider=gdelt). "
        "Optional/unreliable; may be empty."
    ),
)
def raw_gdelt(context) -> dict[str, str]:
    """Collect GDELT TimelineVol (optional; may produce zero rows without failing)."""

    run_date = resolve_run_date(context.partition_key)
    tick_now = _partition_tick_now_utc(run_date)
    batch_id = generate_batch_id(tick_now)
    parts = split_batch_parts(batch_id)
    rt = get_runtime_config()
    end_time = tick_now
    cursor = read_cursor("gdelt", "narratives=default")
    window = compute_window(
        now=end_time,
        cadence=timedelta(seconds=rt.cadence.gdelt_seconds),
        overlap=timedelta(minutes=5),
        cursor=cursor,
    )
    start_time, end_time = _clamp_window_to_partition(window.start_time, window.end_time, run_date)
    timeline, gd_meta = collect_gdelt_raw_window(start_time=start_time, end_time=end_time)
    key = microbatch_key("raw", "provider=gdelt", parts.date, parts.hour, batch_id, ".jsonl")
    write_jsonl_records(timeline, key)
    write_cursor(
        IngestionCursor(
            source="gdelt",
            key="narratives=default",
            last_successful_event_time=gd_meta.get("max_event_time"),
            last_batch_id=batch_id,
            status="ok",
        )
    )

    written = {
        "raw_gdelt_timeline": key,
        "source_batch_id": batch_id,
        "source_window_start": str(gd_meta.get("start_time") or ""),
        "source_window_end": str(gd_meta.get("end_time") or ""),
    }
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": len(timeline),
            "batch_id": batch_id,
            "source_window": MetadataValue.json(
                {"start_time": gd_meta.get("start_time"), "end_time": gd_meta.get("end_time")}
            ),
        }
    )
    return written


def _safe_read_jsonl(key: str) -> list[dict]:
    try:
        return read_jsonl_records(key)
    except Exception:
        return []


@asset(
    partitions_def=partitions_def,
    deps=[raw_polymarket],
    description="Normalize Polymarket raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_polymarket(context, raw_polymarket: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date(context.partition_key)
    markets_key = raw_polymarket["raw_polymarket_markets"]
    prices_key = raw_polymarket["raw_polymarket_prices"]
    source_batch_id = raw_polymarket.get("source_batch_id") or ""
    source_window_start = raw_polymarket.get("source_window_start") or ""
    source_window_end = raw_polymarket.get("source_window_end") or ""
    markets = _safe_read_jsonl(markets_key)
    prices = _safe_read_jsonl(prices_key)

    batch_id = generate_batch_id(_partition_tick_now_utc(run_date))
    parts = split_batch_parts(batch_id)
    markets_df = _with_lineage(normalize_markets(markets), raw_polymarket)
    prices_df = _with_lineage(normalize_price_snapshots(prices), raw_polymarket)

    out_markets_key = microbatch_key(
        "bronze", "provider=polymarket", parts.date, parts.hour, batch_id, "_markets.parquet"
    )
    out_prices_key = microbatch_key(
        "bronze", "provider=polymarket", parts.date, parts.hour, batch_id, "_prices.parquet"
    )
    write_parquet_df(markets_df, out_markets_key)
    write_parquet_df(prices_df, out_prices_key)

    written = {
        "bronze_polymarket_markets": out_markets_key,
        "bronze_polymarket_prices": out_prices_key,
        "source_batch_id": source_batch_id,
        "source_window_start": source_window_start,
        "source_window_end": source_window_end,
    }
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "markets_rows": int(getattr(markets_df, "height", 0)),
            "prices_rows": int(getattr(prices_df, "height", 0)),
            "raw_used": MetadataValue.json(
                {"raw_polymarket_markets": markets_key, "raw_polymarket_prices": prices_key}
            ),
            "batch_id": batch_id,
            "dup_markets": MetadataValue.json(_dup_metrics(markets_df, ["market_id"])),
            "dup_prices": MetadataValue.json(_dup_metrics(prices_df, ["timestamp", "market_id", "outcome"])),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    deps=[raw_binance],
    description="Normalize Binance raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_binance(context, raw_binance: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date(context.partition_key)
    chosen = raw_binance["raw_binance_klines"]
    source_batch_id = raw_binance.get("source_batch_id") or ""
    source_window_start = raw_binance.get("source_window_start") or ""
    source_window_end = raw_binance.get("source_window_end") or ""
    klines = _safe_read_jsonl(chosen)
    bronze_df = _with_lineage(normalize_klines(klines), raw_binance)

    batch_id = generate_batch_id(_partition_tick_now_utc(run_date))
    parts = split_batch_parts(batch_id)
    key = microbatch_key("bronze", "provider=binance", parts.date, parts.hour, batch_id, ".parquet")
    write_parquet_df(bronze_df, key)

    written = {
        "bronze_binance_klines": key,
        "source_batch_id": source_batch_id,
        "source_window_start": source_window_start,
        "source_window_end": source_window_end,
    }
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(bronze_df, "height", 0)),
            "raw_used": chosen,
            "batch_id": batch_id,
            "dup": MetadataValue.json(_dup_metrics(bronze_df, ["timestamp", "symbol", "interval"])),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    deps=[raw_gdelt],
    description="Normalize GDELT raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_gdelt(context, raw_gdelt: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date(context.partition_key)
    chosen = raw_gdelt["raw_gdelt_timeline"]
    source_batch_id = raw_gdelt.get("source_batch_id") or ""
    source_window_start = raw_gdelt.get("source_window_start") or ""
    source_window_end = raw_gdelt.get("source_window_end") or ""
    timeline = _safe_read_jsonl(chosen)
    bronze_df = _with_lineage(normalize_timeline(timeline), raw_gdelt)

    batch_id = generate_batch_id(_partition_tick_now_utc(run_date))
    parts = split_batch_parts(batch_id)
    key = microbatch_key("bronze", "provider=gdelt", parts.date, parts.hour, batch_id, ".parquet")
    write_parquet_df(bronze_df, key)

    written = {
        "bronze_gdelt_timeline": key,
        "source_batch_id": source_batch_id,
        "source_window_start": source_window_start,
        "source_window_end": source_window_end,
    }
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(bronze_df, "height", 0)),
            "raw_used": chosen,
            "batch_id": batch_id,
            "dup": MetadataValue.json(_dup_metrics(bronze_df, ["timestamp", "narrative", "query", "source"])),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    deps=[bronze_polymarket],
    description="Build silver belief_price_snapshots from bronze Polymarket prices.",
)
def silver_belief_price_snapshots(context, bronze_polymarket: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date(context.partition_key)
    prices_df = read_parquet_df(bronze_polymarket["bronze_polymarket_prices"])
    belief_df = to_belief_price_snapshots(prices_df)
    belief_df = _with_lineage(belief_df, bronze_polymarket)

    batch_id = generate_batch_id(_partition_tick_now_utc(run_date))
    parts = split_batch_parts(batch_id)
    key = microbatch_key(
        "silver", "belief_price_snapshots", parts.date, parts.hour, batch_id, ".parquet"
    )
    write_parquet_df(belief_df, key)

    written = {"silver_belief_price_snapshots": key}
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(belief_df, "height", 0)),
            "batch_id": batch_id,
            "dup": MetadataValue.json(_dup_metrics(belief_df, ["timestamp", "market_id", "outcome"])),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    deps=[bronze_binance],
    description="Build silver crypto_candles_1m from bronze Binance klines.",
)
def silver_crypto_candles_1m(context, bronze_binance: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date(context.partition_key)
    klines_df = read_parquet_df(bronze_binance["bronze_binance_klines"])
    candles_df = to_crypto_candles_1m(klines_df)
    candles_df = _with_lineage(candles_df, bronze_binance)

    batch_id = generate_batch_id(_partition_tick_now_utc(run_date))
    parts = split_batch_parts(batch_id)
    key = microbatch_key("silver", "crypto_candles_1m", parts.date, parts.hour, batch_id, ".parquet")
    write_parquet_df(candles_df, key)

    written = {"silver_crypto_candles_1m": key}
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(candles_df, "height", 0)),
            "batch_id": batch_id,
            "dup": MetadataValue.json(_dup_metrics(candles_df, ["timestamp", "asset"])),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    deps=[bronze_gdelt],
    description="Build silver narrative_counts from bronze GDELT timeline series (may be empty).",
)
def silver_narrative_counts(context, bronze_gdelt: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date(context.partition_key)
    timeline_df = read_parquet_df(bronze_gdelt["bronze_gdelt_timeline"])
    counts_df = to_narrative_counts(timeline_df)
    counts_df = _with_lineage(counts_df, bronze_gdelt)

    batch_id = generate_batch_id(_partition_tick_now_utc(run_date))
    parts = split_batch_parts(batch_id)
    key = microbatch_key("silver", "narrative_counts", parts.date, parts.hour, batch_id, ".parquet")
    write_parquet_df(counts_df, key)

    written = {"silver_narrative_counts": key}
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(counts_df, "height", 0)),
            "batch_id": batch_id,
            "dup": MetadataValue.json(_dup_metrics(counts_df, ["timestamp", "narrative", "query", "source"])),
        }
    )
    return written


@multi_asset(
    partitions_def=partitions_def,
    deps=[silver_belief_price_snapshots, silver_crypto_candles_1m, silver_narrative_counts],
    outs={"gold_training_examples": AssetOut(), "gold_live_signals": AssetOut()},
    description="Build gold tables (training_examples + live_signals) from silver inputs.",
)
def gold_tables(
    context,
    silver_belief_price_snapshots: dict[str, str],
    silver_crypto_candles_1m: dict[str, str],
    silver_narrative_counts: dict[str, str],
) -> tuple[Output[dict[str, str]], Output[dict[str, str]]]:
    """Build gold tables once and expose both outputs as distinct assets."""

    run_date = resolve_run_date(context.partition_key)
    written = build_gold_tables(
        run_date=run_date.isoformat(),
        belief_key=silver_belief_price_snapshots["silver_belief_price_snapshots"],
        candles_key=silver_crypto_candles_1m["silver_crypto_candles_1m"],
        narrative_key=silver_narrative_counts["silver_narrative_counts"],
    )

    # build_gold_tables returns keys: training_examples, live_signals
    training_key = written["training_examples"]
    live_key = written["live_signals"]

    md = {"run_date": run_date.isoformat(), "written_keys": ["training_examples", "live_signals"]}
    out_training = Output(
        value={"gold_training_examples": training_key},
        metadata={**md, "s3_key": training_key},
    )
    out_live = Output(
        value={"gold_live_signals": live_key},
        metadata={**md, "s3_key": live_key},
    )
    return out_training, out_live


@asset(
    partitions_def=partitions_def,
    deps=[
        AssetDep(AssetKey("gold_training_examples")),
        AssetDep(AssetKey("gold_live_signals")),
    ],
    description="Run Soda Core checks against DuckDB external Parquet views for this partition.",
)
def soda_data_quality(context) -> dict[str, Any]:
    run_date = resolve_run_date(context.partition_key)
    summary = run_soda_checks(run_date=run_date.isoformat())
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "passed": bool(summary.get("passed")),
            "failed_checks": MetadataValue.json(summary.get("failed_checks") or []),
            "report_paths": MetadataValue.json(summary.get("report_paths") or {}),
        }
    )
    return summary


@asset(
    partitions_def=partitions_def,
    deps=[
        soda_data_quality,
        AssetDep(AssetKey("gold_training_examples")),
        AssetDep(AssetKey("gold_live_signals")),
    ],
    description="Detect domain-specific pipeline/data issues and write markdown/json reports.",
)
def data_issues(context) -> list[dict]:
    run_date = resolve_run_date(context.partition_key)
    issues = detect_data_issues(run_date=run_date.isoformat())
    paths = write_data_issues_reports(issues)
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "issues_count": len(issues),
            "report_paths": MetadataValue.json(paths),
        }
    )
    return issues


@asset(
    partitions_def=partitions_def,
    deps=[data_issues, soda_data_quality],
    description="Write a lightweight markdown index that links to Soda + data-issues reports.",
)
def markdown_reports(
    context, data_issues: list[dict], soda_data_quality: dict[str, Any]
) -> dict[str, str]:
    """Produce a small index markdown that links to quality + issues outputs."""

    run_date = resolve_run_date(context.partition_key)
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    index_path = reports_dir / "index.md"

    soda_paths = soda_data_quality.get("report_paths") or {}
    issues_json = "reports/data_issues.json"
    issues_md = "reports/data_issues.md"

    lines = [
        f"# Reports ({run_date.isoformat()})",
        "",
        "## Data quality (Soda)",
        f"- Output: `{soda_paths.get('output_txt', 'reports/soda_scan_output.txt')}`",
        f"- Summary: `{soda_paths.get('summary_json', 'reports/soda_scan_summary.json')}`",
        "",
        "## Data issues (domain detector)",
        f"- Markdown: `{issues_md}`",
        f"- JSON: `{issues_json}`",
        "",
        "## Quick stats",
        f"- Issues: {len(data_issues)}",
        f"- Soda passed: {bool(soda_data_quality.get('passed'))}",
        "",
    ]
    index_path.write_text("\n".join(lines), encoding="utf-8")

    out = {"index_md": str(index_path)}
    context.add_output_metadata(
        {"run_date": run_date.isoformat(), "report_paths": MetadataValue.json(out)}
    )
    return out


ALL_ASSETS = [
    raw_polymarket,
    raw_binance,
    raw_gdelt,
    bronze_polymarket,
    bronze_binance,
    bronze_gdelt,
    silver_belief_price_snapshots,
    silver_crypto_candles_1m,
    silver_narrative_counts,
    gold_tables,
    soda_data_quality,
    data_issues,
    markdown_reports,
]
