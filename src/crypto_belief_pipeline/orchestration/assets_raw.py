"""Raw-layer Dagster assets: sample inputs + live staging/canonical collectors."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from crypto_belief_pipeline.collectors.binance import collect_binance_raw
from crypto_belief_pipeline.collectors.gdelt import collect_gdelt_raw_window
from crypto_belief_pipeline.collectors.kalshi import collect_kalshi_raw
from crypto_belief_pipeline.collectors.polymarket import collect_polymarket_raw
from crypto_belief_pipeline.config import get_runtime_config, get_settings
from crypto_belief_pipeline.lake.batches import generate_batch_id, split_batch_parts
from crypto_belief_pipeline.lake.paths import microbatch_key, partition_path
from crypto_belief_pipeline.lake.write import write_jsonl_records
from crypto_belief_pipeline.orchestration._helpers import (
    _clamp_window_to_partition,
    _k,
    _partition_tick_now_utc,
    _read_yaml_mapping,
    _safe_read_jsonl,
    _sample_guardrails,
    daily_partitions_def,
    hourly_partitions_def,
    raw_bronze_minute_partitions_def,
)
from crypto_belief_pipeline.orchestration.raw_inputs_from_lake import (
    list_raw_binance_for_partition_window,
    list_raw_gdelt_for_partition_window,
    list_raw_kalshi_for_partition_window,
    list_raw_polymarket_for_partition_window,
)
from crypto_belief_pipeline.orchestration.resources import (
    resolve_partition_window_from_context,
    resolve_run_date_from_context,
)
from crypto_belief_pipeline.orchestration.windows import compute_window
from crypto_belief_pipeline.sample_data import load_sample_jsonl
from dagster import MetadataValue, asset


def _safe_partition_slug(partition_key: str) -> str:
    return partition_key.replace(":", "-")


def _canonical_raw_output_key(*, dataset: str, run_date, partition_key: str) -> str:
    prefix = partition_path("raw", dataset, run_date)
    return f"{prefix}/partition={_safe_partition_slug(partition_key)}/data.jsonl"


def _records_for_hour_window(keys: list[str], *, bucket: str | None) -> list[dict]:
    out: list[dict] = []
    for key in sorted(keys):
        out.extend(_safe_read_jsonl(key, bucket=bucket))
    return out


def _batch_ts_from_source_batch_id(batch_id: str) -> datetime | None:
    if not batch_id:
        return None
    core = batch_id.split("_", 1)[0]
    try:
        return datetime.strptime(core, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError:
        return None


def _needs_api_fallback_for_incomplete_hour(
    *,
    candidates: list[dict[str, str]],
    partition_start: datetime,
    partition_end: datetime,
    cadence_seconds: int,
) -> bool:
    """Return True when any expected microbatch slot appears missing.

    Slots are generated from `[partition_start, partition_end)` at cadence granularity,
    and each slot is considered covered when at least one candidate batch timestamp
    falls inside that slot window.
    """

    if cadence_seconds <= 0:
        return not candidates
    total_seconds = max(int((partition_end - partition_start).total_seconds()), 0)
    if total_seconds == 0:
        return not candidates

    expected_slots = list(range(0, total_seconds, cadence_seconds))
    if not expected_slots:
        expected_slots = [0]

    batch_ts = [
        _batch_ts_from_source_batch_id(it.get("source_batch_id") or "") for it in candidates
    ]
    if not any(ts is not None for ts in batch_ts):
        return True

    for offset in expected_slots:
        slot_start = partition_start + timedelta(seconds=offset)
        slot_end = min(slot_start + timedelta(seconds=cadence_seconds), partition_end)
        if not any(ts is not None and slot_start <= ts < slot_end for ts in batch_ts):
            return True
    return False


@asset(
    partitions_def=daily_partitions_def,
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
    name="raw_polymarket_staging",
    partitions_def=raw_bronze_minute_partitions_def,
    description=("Minute staging ingest: collect Polymarket live markets + prices into raw JSONL."),
)
def raw_polymarket_staging(context) -> dict[str, str]:
    """Collect Polymarket markets + prices and write raw JSONL to the lake."""

    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    tick_now = (
        (partition_window.end - timedelta(seconds=1))
        if partition_window is not None
        else _partition_tick_now_utc(run_date)
    )
    batch_id = generate_batch_id(tick_now, uniqueness=str(getattr(context, "run_id", ""))[:8])
    parts = split_batch_parts(batch_id)
    rt = get_runtime_config()
    end_time = tick_now
    window = compute_window(
        now=end_time,
        cadence=timedelta(seconds=rt.cadence.polymarket_prices_seconds),
        overlap=timedelta(minutes=1),
    )
    start_time, end_time = _clamp_window_to_partition(window.start_time, window.end_time, run_date)
    if partition_window is not None:
        start_time = max(start_time, partition_window.start)
        end_time = min(end_time, partition_window.end)
        if start_time >= end_time:
            start_time = partition_window.start
            end_time = partition_window.end
    cfg = _read_yaml_mapping("config/markets_keywords.yaml").get("polymarket") or {}
    keywords = cfg.get("keywords") or []
    active = bool(cfg.get("active", True))
    closed = bool(cfg.get("closed", False))

    markets, prices, pm_meta = collect_polymarket_raw(
        limit=int(cfg.get("limit", 100)),
        keywords=list(keywords) if isinstance(keywords, list) else [],
        start_time=start_time,
        end_time=end_time,
        snapshot_time=tick_now,
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
    lake_bucket = get_settings().s3_bucket

    written = {
        "raw_polymarket_markets": markets_key,
        "raw_polymarket_prices": prices_key,
        "source_batch_id": batch_id,
        "source_window_start": str(pm_meta.get("start_time") or ""),
        "source_window_end": str(pm_meta.get("end_time") or ""),
        "lake_bucket": lake_bucket,
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
            "window_start_utc": start_time.isoformat(),
            "window_end_utc": end_time.isoformat(),
            "window_closed_open": "[start,end)",
            "missing_updatedAt_count": int(pm_meta.get("missing_updatedAt_count") or 0),
        }
    )
    return written


@asset(
    name="raw_binance_staging",
    partitions_def=raw_bronze_minute_partitions_def,
    description="Minute staging ingest: collect Binance live klines into raw JSONL.",
)
def raw_binance_staging(context) -> dict[str, str]:
    """Collect recent Binance klines and write raw JSONL to the lake."""

    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    tick_now = (
        (partition_window.end - timedelta(seconds=1))
        if partition_window is not None
        else _partition_tick_now_utc(run_date)
    )
    batch_id = generate_batch_id(tick_now, uniqueness=str(getattr(context, "run_id", ""))[:8])
    parts = split_batch_parts(batch_id)
    rt = get_runtime_config()
    end_time = tick_now
    window = compute_window(
        now=end_time,
        cadence=timedelta(seconds=rt.cadence.binance_raw_seconds),
        overlap=timedelta(minutes=1),
    )
    start_time, end_time = _clamp_window_to_partition(window.start_time, window.end_time, run_date)
    if partition_window is not None:
        start_time = max(start_time, partition_window.start)
        end_time = min(end_time, partition_window.end)
        if start_time >= end_time:
            start_time = partition_window.start
            end_time = partition_window.end
    klines, bn_meta = collect_binance_raw(limit=1000, start_time=start_time, end_time=end_time)
    key = microbatch_key("raw", "provider=binance", parts.date, parts.hour, batch_id, ".jsonl")
    write_jsonl_records(klines, key)
    lake_bucket = get_settings().s3_bucket

    written = {
        "raw_binance_klines": key,
        "source_batch_id": batch_id,
        "source_window_start": str(bn_meta.get("start_time") or ""),
        "source_window_end": str(bn_meta.get("end_time") or ""),
        "lake_bucket": lake_bucket,
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
            "window_start_utc": start_time.isoformat(),
            "window_end_utc": end_time.isoformat(),
            "window_closed_open": "[start,end)",
        }
    )
    return written


@asset(
    name="raw_gdelt_staging",
    partitions_def=raw_bronze_minute_partitions_def,
    description=(
        "Minute staging ingest: collect GDELT TimelineVol into raw JSONL. "
        "Optional/unreliable; may be empty."
    ),
)
def raw_gdelt_staging(context) -> dict[str, str]:
    """Collect GDELT TimelineVol (optional; may produce zero rows without failing)."""

    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    tick_now = (
        (partition_window.end - timedelta(seconds=1))
        if partition_window is not None
        else _partition_tick_now_utc(run_date)
    )
    batch_id = generate_batch_id(tick_now, uniqueness=str(getattr(context, "run_id", ""))[:8])
    parts = split_batch_parts(batch_id)
    rt = get_runtime_config()
    end_time = tick_now
    window = compute_window(
        now=end_time,
        cadence=timedelta(seconds=rt.cadence.gdelt_seconds),
        overlap=timedelta(minutes=5),
    )
    start_time, end_time = _clamp_window_to_partition(window.start_time, window.end_time, run_date)
    if partition_window is not None:
        start_time = max(start_time, partition_window.start)
        end_time = min(end_time, partition_window.end)
        if start_time >= end_time:
            start_time = partition_window.start
            end_time = partition_window.end
    timeline, gd_meta = collect_gdelt_raw_window(start_time=start_time, end_time=end_time)
    key = microbatch_key("raw", "provider=gdelt", parts.date, parts.hour, batch_id, ".jsonl")
    write_jsonl_records(timeline, key)
    lake_bucket = get_settings().s3_bucket

    written = {
        "raw_gdelt_timeline": key,
        "source_batch_id": batch_id,
        "source_window_start": str(gd_meta.get("start_time") or ""),
        "source_window_end": str(gd_meta.get("end_time") or ""),
        "lake_bucket": lake_bucket,
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
            "window_start_utc": start_time.isoformat(),
            "window_end_utc": end_time.isoformat(),
            "window_closed_open": "[start,end)",
        }
    )
    return written


@asset(
    name="raw_kalshi_staging",
    partitions_def=raw_bronze_minute_partitions_def,
    description="Minute staging ingest: Kalshi markets/events/series/trades/orderbooks/candles.",
)
def raw_kalshi_staging(context) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    tick_now = (
        (partition_window.end - timedelta(seconds=1))
        if partition_window is not None
        else _partition_tick_now_utc(run_date)
    )
    batch_id = generate_batch_id(tick_now, uniqueness=str(getattr(context, "run_id", ""))[:8])
    parts = split_batch_parts(batch_id)
    rt = get_runtime_config()
    end_time = tick_now
    window = compute_window(
        now=end_time,
        cadence=timedelta(seconds=rt.cadence.kalshi_raw_seconds),
        overlap=timedelta(minutes=2),
    )
    start_time, end_time = _clamp_window_to_partition(window.start_time, window.end_time, run_date)
    if partition_window is not None:
        start_time = max(start_time, partition_window.start)
        end_time = min(end_time, partition_window.end)
        if start_time >= end_time:
            start_time = partition_window.start
            end_time = partition_window.end
    cfg = _read_yaml_mapping("config/kalshi_keywords.yaml")
    payloads, k_meta = collect_kalshi_raw(
        start_time=start_time,
        end_time=end_time,
        snapshot_time=tick_now,
        ingest_batch_id=batch_id,
        cfg=cfg,
    )

    def _mk(suffix: str) -> str:
        return microbatch_key(
            "raw", "provider=kalshi", parts.date, parts.hour, batch_id, suffix
        )

    keys = {
        "raw_kalshi_markets": _mk("_markets.jsonl"),
        "raw_kalshi_events": _mk("_events.jsonl"),
        "raw_kalshi_series": _mk("_series.jsonl"),
        "raw_kalshi_trades": _mk("_trades.jsonl"),
        "raw_kalshi_orderbooks": _mk("_orderbooks.jsonl"),
        "raw_kalshi_candlesticks": _mk("_candlesticks.jsonl"),
    }
    write_jsonl_records(payloads["markets"], keys["raw_kalshi_markets"])
    write_jsonl_records(payloads["events"], keys["raw_kalshi_events"])
    write_jsonl_records(payloads["series"], keys["raw_kalshi_series"])
    write_jsonl_records(payloads["trades"], keys["raw_kalshi_trades"])
    write_jsonl_records(payloads["orderbooks"], keys["raw_kalshi_orderbooks"])
    write_jsonl_records(payloads["candlesticks"], keys["raw_kalshi_candlesticks"])
    lake_bucket = get_settings().s3_bucket

    written = {
        **keys,
        "source_batch_id": batch_id,
        "source_window_start": str(k_meta.get("start_time") or ""),
        "source_window_end": str(k_meta.get("end_time") or ""),
        "lake_bucket": lake_bucket,
    }
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(keys.keys()),
            "batch_id": batch_id,
            "kalshi_meta": MetadataValue.json(k_meta),
            "window_start_utc": start_time.isoformat(),
            "window_end_utc": end_time.isoformat(),
            "window_closed_open": "[start,end)",
        }
    )
    return written


@asset(
    name="raw_polymarket",
    partitions_def=hourly_partitions_def,
    deps=[raw_polymarket_staging],
    description=(
        "Canonical hourly Polymarket raw snapshot derived from minute staging microbatches."
    ),
)
def raw_polymarket_hourly(context) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    if partition_window is None:
        raise ValueError("raw_polymarket hourly asset requires a partition window")
    candidates, read_bucket = list_raw_polymarket_for_partition_window(
        partition_start=partition_window.start,
        partition_end=partition_window.end,
    )
    rt = get_runtime_config()
    fallback_for_missing = _needs_api_fallback_for_incomplete_hour(
        candidates=candidates,
        partition_start=partition_window.start,
        partition_end=partition_window.end,
        cadence_seconds=int(rt.cadence.polymarket_prices_seconds),
    )
    fetched_from_api = False
    if candidates and not fallback_for_missing:
        markets_keys = [it["raw_polymarket_markets"] for it in candidates]
        prices_keys = [it["raw_polymarket_prices"] for it in candidates]
        markets = _records_for_hour_window(markets_keys, bucket=read_bucket)
        prices = _records_for_hour_window(prices_keys, bucket=read_bucket)
        last_batch_id = candidates[-1]["source_batch_id"] if candidates else ""
    else:
        cfg = _read_yaml_mapping("config/markets_keywords.yaml").get("polymarket") or {}
        keywords = cfg.get("keywords") or []
        active = bool(cfg.get("active", True))
        closed = bool(cfg.get("closed", False))
        markets, prices, _pm_meta = collect_polymarket_raw(
            limit=int(cfg.get("limit", 100)),
            keywords=list(keywords) if isinstance(keywords, list) else [],
            start_time=partition_window.start,
            end_time=partition_window.end,
            snapshot_time=partition_window.end - timedelta(seconds=1),
            active=active,
            closed=closed,
        )
        last_batch_id = generate_batch_id(
            partition_window.end - timedelta(seconds=1),
            uniqueness=f"backfill{partition_window.partition_key}",
        )
        fetched_from_api = True

    markets_key = _canonical_raw_output_key(
        dataset="provider=polymarket_markets",
        run_date=run_date,
        partition_key=partition_window.partition_key,
    )
    prices_key = _canonical_raw_output_key(
        dataset="provider=polymarket_prices",
        run_date=run_date,
        partition_key=partition_window.partition_key,
    )
    write_jsonl_records(markets, markets_key, bucket=read_bucket)
    write_jsonl_records(prices, prices_key, bucket=read_bucket)
    written = {
        "raw_polymarket_markets": markets_key,
        "raw_polymarket_prices": prices_key,
        "source_batch_id": last_batch_id,
        "source_window_start": partition_window.start.isoformat(),
        "source_window_end": partition_window.end.isoformat(),
        "lake_bucket": read_bucket,
    }
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "window_start_utc": partition_window.start.isoformat(),
            "window_end_utc": partition_window.end.isoformat(),
            "window_closed_open": "[start,end)",
            "input_microbatches_seen": len(candidates),
            "input_microbatches_processed": len(candidates),
            "markets_rows": len(markets),
            "prices_rows": len(prices),
            "data_source": "api_backfill" if fetched_from_api else "staging_compaction",
            "fallback_triggered_for_missing_microbatches": bool(fallback_for_missing),
        }
    )
    return written


@asset(
    name="raw_binance",
    partitions_def=hourly_partitions_def,
    deps=[raw_binance_staging],
    description="Canonical hourly Binance raw snapshot derived from minute staging microbatches.",
)
def raw_binance_hourly(context) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    if partition_window is None:
        raise ValueError("raw_binance hourly asset requires a partition window")
    candidates, read_bucket = list_raw_binance_for_partition_window(
        partition_start=partition_window.start,
        partition_end=partition_window.end,
    )
    rt = get_runtime_config()
    fallback_for_missing = _needs_api_fallback_for_incomplete_hour(
        candidates=candidates,
        partition_start=partition_window.start,
        partition_end=partition_window.end,
        cadence_seconds=int(rt.cadence.binance_raw_seconds),
    )
    fetched_from_api = False
    if candidates and not fallback_for_missing:
        keys = [it["raw_binance_klines"] for it in candidates]
        klines = _records_for_hour_window(keys, bucket=read_bucket)
        last_batch_id = candidates[-1]["source_batch_id"] if candidates else ""
    else:
        # Backfill safety: if minute staging is missing for this hour, fetch directly
        # from Binance using the exact canonical partition window.
        klines, _bn_meta = collect_binance_raw(
            limit=1000,
            start_time=partition_window.start,
            end_time=partition_window.end,
        )
        last_batch_id = generate_batch_id(
            partition_window.end - timedelta(seconds=1),
            uniqueness=f"backfill{partition_window.partition_key}",
        )
        fetched_from_api = True
    out_key = _canonical_raw_output_key(
        dataset="provider=binance",
        run_date=run_date,
        partition_key=partition_window.partition_key,
    )
    write_jsonl_records(klines, out_key, bucket=read_bucket)
    written = {
        "raw_binance_klines": out_key,
        "source_batch_id": last_batch_id,
        "source_window_start": partition_window.start.isoformat(),
        "source_window_end": partition_window.end.isoformat(),
        "lake_bucket": read_bucket,
    }
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "window_start_utc": partition_window.start.isoformat(),
            "window_end_utc": partition_window.end.isoformat(),
            "window_closed_open": "[start,end)",
            "input_microbatches_seen": len(candidates),
            "input_microbatches_processed": len(candidates),
            "rows": len(klines),
            "data_source": "api_backfill" if fetched_from_api else "staging_compaction",
            "fallback_triggered_for_missing_microbatches": bool(fallback_for_missing),
        }
    )
    return written


@asset(
    name="raw_gdelt",
    partitions_def=hourly_partitions_def,
    deps=[raw_gdelt_staging],
    description="Canonical hourly GDELT raw snapshot derived from minute staging microbatches.",
)
def raw_gdelt_hourly(context) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    if partition_window is None:
        raise ValueError("raw_gdelt hourly asset requires a partition window")
    candidates, read_bucket = list_raw_gdelt_for_partition_window(
        partition_start=partition_window.start,
        partition_end=partition_window.end,
    )
    rt = get_runtime_config()
    fallback_for_missing = _needs_api_fallback_for_incomplete_hour(
        candidates=candidates,
        partition_start=partition_window.start,
        partition_end=partition_window.end,
        cadence_seconds=int(rt.cadence.gdelt_seconds),
    )
    fetched_from_api = False
    gd_meta: dict = {}
    if candidates and not fallback_for_missing:
        keys = [it["raw_gdelt_timeline"] for it in candidates]
        timeline = _records_for_hour_window(keys, bucket=read_bucket)
        last_batch_id = candidates[-1]["source_batch_id"] if candidates else ""
    else:
        timeline, gd_meta = collect_gdelt_raw_window(
            start_time=partition_window.start,
            end_time=partition_window.end,
        )
        last_batch_id = generate_batch_id(
            partition_window.end - timedelta(seconds=1),
            uniqueness=f"backfill{partition_window.partition_key}",
        )
        fetched_from_api = True
    out_key = _canonical_raw_output_key(
        dataset="provider=gdelt",
        run_date=run_date,
        partition_key=partition_window.partition_key,
    )
    write_jsonl_records(timeline, out_key, bucket=read_bucket)
    written = {
        "raw_gdelt_timeline": out_key,
        "source_batch_id": last_batch_id,
        "source_window_start": partition_window.start.isoformat(),
        "source_window_end": partition_window.end.isoformat(),
        "lake_bucket": read_bucket,
    }
    meta_out: dict[str, object] = {
        "run_date": run_date.isoformat(),
        "window_start_utc": partition_window.start.isoformat(),
        "window_end_utc": partition_window.end.isoformat(),
        "window_closed_open": "[start,end)",
        "input_microbatches_seen": len(candidates),
        "input_microbatches_processed": len(candidates),
        "rows": len(timeline),
        "data_source": "api_backfill" if fetched_from_api else "staging_compaction",
        "fallback_triggered_for_missing_microbatches": bool(fallback_for_missing),
    }
    if fetched_from_api and gd_meta:
        meta_out["gdelt_collection"] = MetadataValue.json(
            {
                k: gd_meta.get(k)
                for k in (
                    "gdelt_narratives_attempted",
                    "gdelt_narratives_failed",
                    "gdelt_failure_details",
                    "records",
                )
                if k in gd_meta
            }
        )
    context.add_output_metadata(meta_out)
    return written


@asset(
    name="raw_kalshi",
    partitions_def=hourly_partitions_def,
    deps=[raw_kalshi_staging],
    description="Canonical hourly Kalshi raw snapshot derived from minute staging microbatches.",
)
def raw_kalshi_hourly(context) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    if partition_window is None:
        raise ValueError("raw_kalshi hourly asset requires a partition window")
    candidates, read_bucket = list_raw_kalshi_for_partition_window(
        partition_start=partition_window.start,
        partition_end=partition_window.end,
    )
    rt = get_runtime_config()
    fallback_for_missing = _needs_api_fallback_for_incomplete_hour(
        candidates=candidates,
        partition_start=partition_window.start,
        partition_end=partition_window.end,
        cadence_seconds=int(rt.cadence.kalshi_raw_seconds),
    )
    fetched_from_api = False
    if candidates and not fallback_for_missing:
        markets = _records_for_hour_window(
            [it["raw_kalshi_markets"] for it in candidates], bucket=read_bucket
        )
        events = _records_for_hour_window(
            [it["raw_kalshi_events"] for it in candidates], bucket=read_bucket
        )
        series = _records_for_hour_window(
            [it["raw_kalshi_series"] for it in candidates], bucket=read_bucket
        )
        trades = _records_for_hour_window(
            [it["raw_kalshi_trades"] for it in candidates], bucket=read_bucket
        )
        orderbooks = _records_for_hour_window(
            [it["raw_kalshi_orderbooks"] for it in candidates], bucket=read_bucket
        )
        candlesticks = _records_for_hour_window(
            [it["raw_kalshi_candlesticks"] for it in candidates], bucket=read_bucket
        )
        last_batch_id = candidates[-1]["source_batch_id"] if candidates else ""
    else:
        cfg = _read_yaml_mapping("config/kalshi_keywords.yaml")
        last_batch_id = generate_batch_id(
            partition_window.end - timedelta(seconds=1),
            uniqueness=f"backfill{partition_window.partition_key}",
        )
        payloads, _k_meta = collect_kalshi_raw(
            start_time=partition_window.start,
            end_time=partition_window.end,
            snapshot_time=partition_window.end - timedelta(seconds=1),
            ingest_batch_id=last_batch_id,
            cfg=cfg,
        )
        markets = payloads["markets"]
        events = payloads["events"]
        series = payloads["series"]
        trades = payloads["trades"]
        orderbooks = payloads["orderbooks"]
        candlesticks = payloads["candlesticks"]
        fetched_from_api = True

    keys_out = {
        "raw_kalshi_markets": _canonical_raw_output_key(
            dataset="provider=kalshi_markets",
            run_date=run_date,
            partition_key=partition_window.partition_key,
        ),
        "raw_kalshi_events": _canonical_raw_output_key(
            dataset="provider=kalshi_events",
            run_date=run_date,
            partition_key=partition_window.partition_key,
        ),
        "raw_kalshi_series": _canonical_raw_output_key(
            dataset="provider=kalshi_series",
            run_date=run_date,
            partition_key=partition_window.partition_key,
        ),
        "raw_kalshi_trades": _canonical_raw_output_key(
            dataset="provider=kalshi_trades",
            run_date=run_date,
            partition_key=partition_window.partition_key,
        ),
        "raw_kalshi_orderbooks": _canonical_raw_output_key(
            dataset="provider=kalshi_orderbooks",
            run_date=run_date,
            partition_key=partition_window.partition_key,
        ),
        "raw_kalshi_candlesticks": _canonical_raw_output_key(
            dataset="provider=kalshi_candlesticks",
            run_date=run_date,
            partition_key=partition_window.partition_key,
        ),
    }
    write_jsonl_records(markets, keys_out["raw_kalshi_markets"], bucket=read_bucket)
    write_jsonl_records(events, keys_out["raw_kalshi_events"], bucket=read_bucket)
    write_jsonl_records(series, keys_out["raw_kalshi_series"], bucket=read_bucket)
    write_jsonl_records(trades, keys_out["raw_kalshi_trades"], bucket=read_bucket)
    write_jsonl_records(orderbooks, keys_out["raw_kalshi_orderbooks"], bucket=read_bucket)
    write_jsonl_records(candlesticks, keys_out["raw_kalshi_candlesticks"], bucket=read_bucket)

    written = {
        **keys_out,
        "source_batch_id": last_batch_id,
        "source_window_start": partition_window.start.isoformat(),
        "source_window_end": partition_window.end.isoformat(),
        "lake_bucket": read_bucket,
    }
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "window_start_utc": partition_window.start.isoformat(),
            "window_end_utc": partition_window.end.isoformat(),
            "window_closed_open": "[start,end)",
            "input_microbatches_seen": len(candidates),
            "input_microbatches_processed": len(candidates),
            "markets_rows": len(markets),
            "events_rows": len(events),
            "series_rows": len(series),
            "trades_rows": len(trades),
            "orderbooks_rows": len(orderbooks),
            "candlesticks_rows": len(candlesticks),
            "data_source": "api_backfill" if fetched_from_api else "staging_compaction",
            "fallback_triggered_for_missing_microbatches": bool(fallback_for_missing),
        }
    )
    return written


# Backward-compatible module symbols for existing imports.
raw_polymarket = raw_polymarket_hourly
raw_binance = raw_binance_hourly
raw_gdelt = raw_gdelt_hourly
raw_kalshi = raw_kalshi_hourly


__all__ = [
    "raw_binance",
    "raw_binance_hourly",
    "raw_binance_staging",
    "raw_gdelt",
    "raw_gdelt_hourly",
    "raw_gdelt_staging",
    "raw_kalshi",
    "raw_kalshi_hourly",
    "raw_kalshi_staging",
    "raw_polymarket",
    "raw_polymarket_hourly",
    "raw_polymarket_staging",
    "raw_sample_inputs",
]
