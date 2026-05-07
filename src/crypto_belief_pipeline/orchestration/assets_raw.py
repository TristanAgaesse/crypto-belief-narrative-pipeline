"""Raw-layer Dagster assets: sample inputs + live collectors.

These assets only write JSONL to the lake's `raw/` layer; they do not normalize.
"""

from __future__ import annotations

from datetime import timedelta

from crypto_belief_pipeline.collectors.binance import collect_binance_raw
from crypto_belief_pipeline.collectors.gdelt import collect_gdelt_raw_window
from crypto_belief_pipeline.collectors.polymarket import collect_polymarket_raw
from crypto_belief_pipeline.config import get_runtime_config
from crypto_belief_pipeline.lake.batches import generate_batch_id, split_batch_parts
from crypto_belief_pipeline.lake.paths import microbatch_key, partition_path
from crypto_belief_pipeline.lake.write import write_jsonl_records
from crypto_belief_pipeline.orchestration._helpers import (
    _clamp_window_to_partition,
    _k,
    _partition_tick_now_utc,
    _read_yaml_mapping,
    _sample_guardrails,
    partitions_def,
)
from crypto_belief_pipeline.orchestration.resources import resolve_run_date_from_context
from crypto_belief_pipeline.orchestration.windows import compute_window
from crypto_belief_pipeline.sample_data import load_sample_jsonl
from crypto_belief_pipeline.state.ingestion_cursors import (
    IngestionCursor,
    read_cursor,
    write_cursor,
)
from dagster import MetadataValue, asset


@asset(
    partitions_def=partitions_def,
    description="Write deterministic sample raw JSONL inputs into the lake (raw/).",
)
def raw_sample_inputs(context) -> dict[str, str]:
    """Write sample JSONL into the lake under raw/ (partitioned by run_date)."""

    run_date = resolve_run_date_from_context(context)
    pm_markets = load_sample_jsonl("polymarket_markets_sample.jsonl")
    pm_prices = load_sample_jsonl("polymarket_prices_sample.jsonl")
    bn_klines = load_sample_jsonl("binance_klines_sample.jsonl")
    gd_timeline = load_sample_jsonl("gdelt_timeline_sample.jsonl")

    sample_bucket = _sample_guardrails()
    raw_pm = partition_path("raw", "provider=polymarket", run_date)
    raw_bn = partition_path("raw", "provider=binance", run_date)
    raw_gd = partition_path("raw", "provider=gdelt", run_date)

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
            "sample_bucket": sample_bucket,
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

    run_date = resolve_run_date_from_context(context)
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

    run_date = resolve_run_date_from_context(context)
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

    run_date = resolve_run_date_from_context(context)
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


__all__ = ["raw_binance", "raw_gdelt", "raw_polymarket", "raw_sample_inputs"]
