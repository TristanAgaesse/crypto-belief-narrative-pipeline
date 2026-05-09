"""Bronze + silver Dagster assets: typed normalize + research-ready joins.

Bronze writes typed Parquet from raw JSONL. Silver derives source-aware
contract tables (belief_price_snapshots, crypto_candles_1m, narrative_counts).
"""

from __future__ import annotations

import polars as pl

from crypto_belief_pipeline.contracts import (
    BRONZE_BINANCE_KLINES,
    BRONZE_FEAR_GREED,
    BRONZE_GDELT_TIMELINE,
    BRONZE_POLYMARKET_MARKETS,
    BRONZE_POLYMARKET_PRICES,
    SILVER_BELIEF_PRICE_SNAPSHOTS,
    SILVER_CRYPTO_CANDLES_1M,
    SILVER_FEAR_GREED_DAILY,
    SILVER_FEAR_GREED_REGIME_FEATURES,
    SILVER_KALSHI_CANDLESTICKS,
    SILVER_KALSHI_EVENT_REPRICING_FEATURES,
    SILVER_KALSHI_EVENTS,
    SILVER_KALSHI_MARKET_SNAPSHOTS,
    SILVER_KALSHI_MARKETS,
    SILVER_KALSHI_ORDERBOOK_SNAPSHOTS,
    SILVER_KALSHI_SERIES,
    SILVER_KALSHI_TRADES,
    SILVER_NARRATIVE_COUNTS,
)
from crypto_belief_pipeline.lake.read import read_parquet_df
from crypto_belief_pipeline.lake.write import write_parquet_df
from crypto_belief_pipeline.orchestration._helpers import (
    _dup_metrics,
    _safe_read_jsonl,
    _with_lineage,
    hourly_partitions_def,
)
from crypto_belief_pipeline.orchestration.assets_raw import (
    raw_binance,
    raw_fear_greed,
    raw_gdelt,
    raw_kalshi,
    raw_polymarket,
)
from crypto_belief_pipeline.orchestration.raw_inputs_from_lake import (
    list_raw_binance_for_partition_window,
    list_raw_fear_greed_for_partition_window,
    list_raw_gdelt_for_partition_window,
    list_raw_kalshi_for_partition_window,
    list_raw_polymarket_for_partition_window,
)
from crypto_belief_pipeline.orchestration.resources import (
    resolve_partition_window_from_context,
    resolve_run_date_from_context,
)
from crypto_belief_pipeline.state.processing_watermarks import (
    build_watermark,
    write_processing_watermark,
)
from crypto_belief_pipeline.transform.normalize_binance import (
    normalize_klines,
    to_crypto_candles_1m,
)
from crypto_belief_pipeline.transform.normalize_gdelt import normalize_timeline, to_narrative_counts
from crypto_belief_pipeline.features.kalshi_repricing import build_event_repricing_features
from crypto_belief_pipeline.transform.normalize_kalshi import (
    load_keyword_config,
    normalize_kalshi_candlesticks,
    normalize_kalshi_events,
    normalize_kalshi_markets,
    normalize_kalshi_orderbooks,
    normalize_kalshi_series,
    normalize_kalshi_trades,
    to_silver_kalshi_candlesticks,
    to_silver_kalshi_events,
    to_silver_kalshi_market_snapshots,
    to_silver_kalshi_markets,
    to_silver_kalshi_orderbook_snapshots,
    to_silver_kalshi_series,
    to_silver_kalshi_trades,
)
from crypto_belief_pipeline.transform.normalize_fear_greed import (
    build_regime_features,
    normalize_fear_greed_payload_records,
    to_fear_greed_daily,
)
from crypto_belief_pipeline.transform.normalize_polymarket import (
    normalize_markets,
    normalize_price_snapshots,
    to_belief_price_snapshots,
)
from dagster import MetadataValue, asset


def _safe_partition_slug(partition_key: str) -> str:
    return partition_key.replace(":", "-")


def _canonical_partition_output_key(
    *,
    layer: str,
    dataset: str,
    run_date,
    partition_key: str | None,
) -> str:
    from crypto_belief_pipeline.lake.paths import partition_path

    prefix = partition_path(layer, dataset, run_date)
    if partition_key:
        return f"{prefix}/partition={_safe_partition_slug(partition_key)}/data.parquet"
    return f"{prefix}/data.parquet"


def _read_records_for_keys(keys: list[str], *, bucket: str | None) -> list[dict]:
    out: list[dict] = []
    for key in sorted(keys):
        out.extend(_safe_read_jsonl(key, bucket=bucket))
    return out


@asset(
    partitions_def=hourly_partitions_def,
    deps=[raw_polymarket],
    description="Normalize Polymarket raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_polymarket(context) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    if partition_window is None:
        raise ValueError("bronze_polymarket requires an hourly partition window")
    partition_key = partition_window.partition_key
    partition_start = partition_window.start
    partition_end = partition_window.end
    candidates, read_bucket = list_raw_polymarket_for_partition_window(
        partition_start=partition_start,
        partition_end=partition_end,
        include_canonical_hourly=True,
    )
    markets_keys = [it["raw_polymarket_markets"] for it in candidates]
    prices_keys = [it["raw_polymarket_prices"] for it in candidates]
    markets = _read_records_for_keys(markets_keys, bucket=read_bucket)
    prices = _read_records_for_keys(prices_keys, bucket=read_bucket)
    last_batch_id = candidates[-1]["source_batch_id"] if candidates else ""
    lineage = {
        "source_batch_id": last_batch_id,
        "source_window_start": partition_start.isoformat(),
        "source_window_end": partition_end.isoformat(),
    }
    markets_norm = normalize_markets(markets)
    BRONZE_POLYMARKET_MARKETS.validate(markets_norm)
    prices_norm = normalize_price_snapshots(prices)
    BRONZE_POLYMARKET_PRICES.validate(prices_norm)
    markets_df = _with_lineage(markets_norm, lineage)
    prices_df = _with_lineage(prices_norm, lineage)

    out_markets_key = _canonical_partition_output_key(
        layer="bronze",
        dataset="provider=polymarket_markets",
        run_date=run_date,
        partition_key=partition_key,
    )
    out_prices_key = _canonical_partition_output_key(
        layer="bronze",
        dataset="provider=polymarket_prices",
        run_date=run_date,
        partition_key=partition_key,
    )
    write_parquet_df(markets_df, out_markets_key, bucket=read_bucket)
    write_parquet_df(prices_df, out_prices_key, bucket=read_bucket)

    written = {
        "bronze_polymarket_markets": out_markets_key,
        "bronze_polymarket_prices": out_prices_key,
        "source_batch_id": last_batch_id,
        "source_window_start": partition_start.isoformat(),
        "source_window_end": partition_end.isoformat(),
        "lake_bucket": read_bucket,
    }
    processed_keys = sorted(set(markets_keys + prices_keys))
    write_processing_watermark(
        build_watermark(
            consumer_asset="bronze_polymarket",
            source="polymarket",
            partition_key=partition_key,
            partition_start=partition_start,
            partition_end=partition_end,
            processed_input_keys=processed_keys,
            last_processed_batch_id=last_batch_id or None,
        ),
        bucket=read_bucket,
    )
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "markets_rows": int(getattr(markets_df, "height", 0)),
            "prices_rows": int(getattr(prices_df, "height", 0)),
            "window_start_utc": partition_start.isoformat(),
            "window_end_utc": partition_end.isoformat(),
            "window_closed_open": "[start,end)",
            "input_microbatches_seen": len(candidates),
            "input_microbatches_processed": len(candidates),
            "dup_markets": MetadataValue.json(_dup_metrics(markets_df, ["market_id"])),
            "dup_prices": MetadataValue.json(
                _dup_metrics(prices_df, ["timestamp", "market_id", "outcome"])
            ),
        }
    )
    return written


@asset(
    partitions_def=hourly_partitions_def,
    deps=[raw_binance],
    description="Normalize Binance raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_binance(context) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    if partition_window is None:
        raise ValueError("bronze_binance requires an hourly partition window")
    partition_key = partition_window.partition_key
    partition_start = partition_window.start
    partition_end = partition_window.end
    candidates, read_bucket = list_raw_binance_for_partition_window(
        partition_start=partition_start,
        partition_end=partition_end,
        include_canonical_hourly=True,
    )
    chosen_keys = [it["raw_binance_klines"] for it in candidates]
    klines = _read_records_for_keys(chosen_keys, bucket=read_bucket)
    last_batch_id = candidates[-1]["source_batch_id"] if candidates else ""
    lineage = {
        "source_batch_id": last_batch_id,
        "source_window_start": partition_start.isoformat(),
        "source_window_end": partition_end.isoformat(),
    }
    klines_norm = normalize_klines(klines)
    BRONZE_BINANCE_KLINES.validate(klines_norm)
    bronze_df = _with_lineage(klines_norm, lineage)
    key = _canonical_partition_output_key(
        layer="bronze",
        dataset="provider=binance",
        run_date=run_date,
        partition_key=partition_key,
    )
    write_parquet_df(bronze_df, key, bucket=read_bucket)

    written = {
        "bronze_binance_klines": key,
        "source_batch_id": last_batch_id,
        "source_window_start": partition_start.isoformat(),
        "source_window_end": partition_end.isoformat(),
        "lake_bucket": read_bucket,
    }
    processed_keys = sorted(set(chosen_keys))
    write_processing_watermark(
        build_watermark(
            consumer_asset="bronze_binance",
            source="binance",
            partition_key=partition_key,
            partition_start=partition_start,
            partition_end=partition_end,
            processed_input_keys=processed_keys,
            last_processed_batch_id=last_batch_id or None,
        ),
        bucket=read_bucket,
    )
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(bronze_df, "height", 0)),
            "window_start_utc": partition_start.isoformat(),
            "window_end_utc": partition_end.isoformat(),
            "window_closed_open": "[start,end)",
            "input_microbatches_seen": len(candidates),
            "input_microbatches_processed": len(candidates),
            "dup": MetadataValue.json(_dup_metrics(bronze_df, ["timestamp", "symbol", "interval"])),
        }
    )
    return written


@asset(
    partitions_def=hourly_partitions_def,
    deps=[raw_gdelt],
    description="Normalize GDELT raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_gdelt(context) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    if partition_window is None:
        raise ValueError("bronze_gdelt requires an hourly partition window")
    partition_key = partition_window.partition_key
    partition_start = partition_window.start
    partition_end = partition_window.end
    candidates, read_bucket = list_raw_gdelt_for_partition_window(
        partition_start=partition_start,
        partition_end=partition_end,
        include_canonical_hourly=True,
    )
    chosen_keys = [it["raw_gdelt_timeline"] for it in candidates]
    timeline = _read_records_for_keys(chosen_keys, bucket=read_bucket)
    last_batch_id = candidates[-1]["source_batch_id"] if candidates else ""
    lineage = {
        "source_batch_id": last_batch_id,
        "source_window_start": partition_start.isoformat(),
        "source_window_end": partition_end.isoformat(),
    }
    timeline_norm = normalize_timeline(timeline)
    BRONZE_GDELT_TIMELINE.validate(timeline_norm)
    bronze_df = _with_lineage(timeline_norm, lineage)
    key = _canonical_partition_output_key(
        layer="bronze",
        dataset="provider=gdelt",
        run_date=run_date,
        partition_key=partition_key,
    )
    write_parquet_df(bronze_df, key, bucket=read_bucket)

    written = {
        "bronze_gdelt_timeline": key,
        "source_batch_id": last_batch_id,
        "source_window_start": partition_start.isoformat(),
        "source_window_end": partition_end.isoformat(),
        "lake_bucket": read_bucket,
    }
    processed_keys = sorted(set(chosen_keys))
    write_processing_watermark(
        build_watermark(
            consumer_asset="bronze_gdelt",
            source="gdelt",
            partition_key=partition_key,
            partition_start=partition_start,
            partition_end=partition_end,
            processed_input_keys=processed_keys,
            last_processed_batch_id=last_batch_id or None,
        ),
        bucket=read_bucket,
    )
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(bronze_df, "height", 0)),
            "window_start_utc": partition_start.isoformat(),
            "window_end_utc": partition_end.isoformat(),
            "window_closed_open": "[start,end)",
            "input_microbatches_seen": len(candidates),
            "input_microbatches_processed": len(candidates),
            "dup": MetadataValue.json(
                _dup_metrics(bronze_df, ["timestamp", "narrative", "query", "source"])
            ),
        }
    )
    return written


@asset(
    partitions_def=hourly_partitions_def,
    deps=[raw_kalshi],
    description="Normalize Kalshi raw JSONL into bronze Parquet (markets/events/series/trades/books/candles).",
)
def bronze_kalshi(context) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    if partition_window is None:
        raise ValueError("bronze_kalshi requires an hourly partition window")
    partition_key = partition_window.partition_key
    partition_start = partition_window.start
    partition_end = partition_window.end
    candidates, read_bucket = list_raw_kalshi_for_partition_window(
        partition_start=partition_start,
        partition_end=partition_end,
        include_canonical_hourly=True,
    )
    markets = _read_records_for_keys(
        [it["raw_kalshi_markets"] for it in candidates], bucket=read_bucket
    )
    events = _read_records_for_keys(
        [it["raw_kalshi_events"] for it in candidates], bucket=read_bucket
    )
    series = _read_records_for_keys(
        [it["raw_kalshi_series"] for it in candidates], bucket=read_bucket
    )
    trades = _read_records_for_keys(
        [it["raw_kalshi_trades"] for it in candidates], bucket=read_bucket
    )
    orderbooks = _read_records_for_keys(
        [it["raw_kalshi_orderbooks"] for it in candidates], bucket=read_bucket
    )
    candles = _read_records_for_keys(
        [it["raw_kalshi_candlesticks"] for it in candidates], bucket=read_bucket
    )
    last_batch_id = candidates[-1]["source_batch_id"] if candidates else ""
    lineage = {
        "source_batch_id": last_batch_id,
        "source_window_start": partition_start.isoformat(),
        "source_window_end": partition_end.isoformat(),
    }
    markets_df = _with_lineage(normalize_kalshi_markets(markets), lineage)
    events_df = _with_lineage(normalize_kalshi_events(events), lineage)
    series_df = _with_lineage(normalize_kalshi_series(series), lineage)
    trades_df = _with_lineage(normalize_kalshi_trades(trades), lineage)
    orderbooks_df = _with_lineage(normalize_kalshi_orderbooks(orderbooks), lineage)
    candles_df = _with_lineage(normalize_kalshi_candlesticks(candles), lineage)

    keys_out = {
        "bronze_kalshi_markets": _canonical_partition_output_key(
            layer="bronze",
            dataset="provider=kalshi_markets",
            run_date=run_date,
            partition_key=partition_key,
        ),
        "bronze_kalshi_events": _canonical_partition_output_key(
            layer="bronze",
            dataset="provider=kalshi_events",
            run_date=run_date,
            partition_key=partition_key,
        ),
        "bronze_kalshi_series": _canonical_partition_output_key(
            layer="bronze",
            dataset="provider=kalshi_series",
            run_date=run_date,
            partition_key=partition_key,
        ),
        "bronze_kalshi_trades": _canonical_partition_output_key(
            layer="bronze",
            dataset="provider=kalshi_trades",
            run_date=run_date,
            partition_key=partition_key,
        ),
        "bronze_kalshi_orderbooks": _canonical_partition_output_key(
            layer="bronze",
            dataset="provider=kalshi_orderbooks",
            run_date=run_date,
            partition_key=partition_key,
        ),
        "bronze_kalshi_candlesticks": _canonical_partition_output_key(
            layer="bronze",
            dataset="provider=kalshi_candlesticks",
            run_date=run_date,
            partition_key=partition_key,
        ),
    }
    write_parquet_df(markets_df, keys_out["bronze_kalshi_markets"], bucket=read_bucket)
    write_parquet_df(events_df, keys_out["bronze_kalshi_events"], bucket=read_bucket)
    write_parquet_df(series_df, keys_out["bronze_kalshi_series"], bucket=read_bucket)
    write_parquet_df(trades_df, keys_out["bronze_kalshi_trades"], bucket=read_bucket)
    write_parquet_df(orderbooks_df, keys_out["bronze_kalshi_orderbooks"], bucket=read_bucket)
    write_parquet_df(candles_df, keys_out["bronze_kalshi_candlesticks"], bucket=read_bucket)

    written = {
        **keys_out,
        "source_batch_id": last_batch_id,
        "source_window_start": partition_start.isoformat(),
        "source_window_end": partition_end.isoformat(),
        "lake_bucket": read_bucket,
    }
    processed = []
    for it in candidates:
        for k in (
            "raw_kalshi_markets",
            "raw_kalshi_events",
            "raw_kalshi_series",
            "raw_kalshi_trades",
            "raw_kalshi_orderbooks",
            "raw_kalshi_candlesticks",
        ):
            processed.append(it[k])
    write_processing_watermark(
        build_watermark(
            consumer_asset="bronze_kalshi",
            source="kalshi",
            partition_key=partition_key,
            partition_start=partition_start,
            partition_end=partition_end,
            processed_input_keys=sorted(set(processed)),
            last_processed_batch_id=last_batch_id or None,
        ),
        bucket=read_bucket,
    )
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(keys_out.keys()),
            "window_start_utc": partition_start.isoformat(),
            "window_end_utc": partition_end.isoformat(),
            "input_microbatches_seen": len(candidates),
            "markets_rows": int(markets_df.height),
            "events_rows": int(events_df.height),
        }
    )
    return written


@asset(
    partitions_def=hourly_partitions_def,
    deps=[bronze_kalshi],
    description="Silver Kalshi markets with crypto relevance scoring.",
)
def silver_kalshi_markets(context, bronze_kalshi: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    bucket = bronze_kalshi.get("lake_bucket")
    mk = read_parquet_df(bronze_kalshi["bronze_kalshi_markets"], bucket=bucket)
    ev = read_parquet_df(bronze_kalshi["bronze_kalshi_events"], bucket=bucket)
    se = read_parquet_df(bronze_kalshi["bronze_kalshi_series"], bucket=bucket)
    cfg = load_keyword_config()
    df = to_silver_kalshi_markets(mk, ev, se, cfg=cfg)
    df = df.with_columns(pl.col("priority").cast(pl.Int64))
    df = _with_lineage(df, bronze_kalshi)
    SILVER_KALSHI_MARKETS.validate(df)
    key = _canonical_partition_output_key(
        layer="silver",
        dataset="kalshi_markets",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(df, key, bucket=bucket)
    return {"silver_kalshi_markets": key, "lake_bucket": bucket} if bucket else {"silver_kalshi_markets": key}


@asset(
    partitions_def=hourly_partitions_def,
    deps=[bronze_kalshi],
    description="Silver Kalshi market snapshots (mid/spread from quotes).",
)
def silver_kalshi_market_snapshots(context, bronze_kalshi: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    bucket = bronze_kalshi.get("lake_bucket")
    mk = read_parquet_df(bronze_kalshi["bronze_kalshi_markets"], bucket=bucket)
    df = to_silver_kalshi_market_snapshots(mk)
    df = _with_lineage(df, bronze_kalshi)
    SILVER_KALSHI_MARKET_SNAPSHOTS.validate(df)
    key = _canonical_partition_output_key(
        layer="silver",
        dataset="kalshi_market_snapshots",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(df, key, bucket=bucket)
    return {"silver_kalshi_market_snapshots": key, "lake_bucket": bucket} if bucket else {
        "silver_kalshi_market_snapshots": key
    }


@asset(
    partitions_def=hourly_partitions_def,
    deps=[bronze_kalshi],
    description="Silver Kalshi events.",
)
def silver_kalshi_events(context, bronze_kalshi: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    bucket = bronze_kalshi.get("lake_bucket")
    ev = read_parquet_df(bronze_kalshi["bronze_kalshi_events"], bucket=bucket)
    df = to_silver_kalshi_events(ev)
    df = _with_lineage(df, bronze_kalshi)
    SILVER_KALSHI_EVENTS.validate(df)
    key = _canonical_partition_output_key(
        layer="silver",
        dataset="kalshi_events",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(df, key, bucket=bucket)
    return {"silver_kalshi_events": key, "lake_bucket": bucket} if bucket else {"silver_kalshi_events": key}


@asset(
    partitions_def=hourly_partitions_def,
    deps=[bronze_kalshi],
    description="Silver Kalshi series metadata.",
)
def silver_kalshi_series(context, bronze_kalshi: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    bucket = bronze_kalshi.get("lake_bucket")
    se = read_parquet_df(bronze_kalshi["bronze_kalshi_series"], bucket=bucket)
    df = to_silver_kalshi_series(se)
    df = _with_lineage(df, bronze_kalshi)
    SILVER_KALSHI_SERIES.validate(df)
    key = _canonical_partition_output_key(
        layer="silver",
        dataset="kalshi_series",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(df, key, bucket=bucket)
    return {"silver_kalshi_series": key, "lake_bucket": bucket} if bucket else {"silver_kalshi_series": key}


@asset(
    partitions_def=hourly_partitions_def,
    deps=[bronze_kalshi],
    description="Silver Kalshi trades (deduped).",
)
def silver_kalshi_trades(context, bronze_kalshi: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    bucket = bronze_kalshi.get("lake_bucket")
    tr = read_parquet_df(bronze_kalshi["bronze_kalshi_trades"], bucket=bucket)
    df = to_silver_kalshi_trades(tr)
    df = _with_lineage(df, bronze_kalshi)
    SILVER_KALSHI_TRADES.validate(df)
    key = _canonical_partition_output_key(
        layer="silver",
        dataset="kalshi_trades",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(df, key, bucket=bucket)
    return {"silver_kalshi_trades": key, "lake_bucket": bucket} if bucket else {"silver_kalshi_trades": key}


@asset(
    partitions_def=hourly_partitions_def,
    deps=[bronze_kalshi],
    description="Silver Kalshi order book snapshots.",
)
def silver_kalshi_orderbook_snapshots(context, bronze_kalshi: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    bucket = bronze_kalshi.get("lake_bucket")
    ob = read_parquet_df(bronze_kalshi["bronze_kalshi_orderbooks"], bucket=bucket)
    df = to_silver_kalshi_orderbook_snapshots(ob)
    df = _with_lineage(df, bronze_kalshi)
    SILVER_KALSHI_ORDERBOOK_SNAPSHOTS.validate(df)
    key = _canonical_partition_output_key(
        layer="silver",
        dataset="kalshi_orderbook_snapshots",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(df, key, bucket=bucket)
    return {"silver_kalshi_orderbook_snapshots": key, "lake_bucket": bucket} if bucket else {
        "silver_kalshi_orderbook_snapshots": key
    }


@asset(
    partitions_def=hourly_partitions_def,
    deps=[bronze_kalshi],
    description="Silver Kalshi candlesticks.",
)
def silver_kalshi_candlesticks(context, bronze_kalshi: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    bucket = bronze_kalshi.get("lake_bucket")
    cd = read_parquet_df(bronze_kalshi["bronze_kalshi_candlesticks"], bucket=bucket)
    df = to_silver_kalshi_candlesticks(cd)
    df = _with_lineage(df, bronze_kalshi)
    SILVER_KALSHI_CANDLESTICKS.validate(df)
    key = _canonical_partition_output_key(
        layer="silver",
        dataset="kalshi_candlesticks",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(df, key, bucket=bucket)
    return {"silver_kalshi_candlesticks": key, "lake_bucket": bucket} if bucket else {
        "silver_kalshi_candlesticks": key
    }


@asset(
    partitions_def=hourly_partitions_def,
    deps=[silver_kalshi_market_snapshots, silver_kalshi_trades, silver_kalshi_markets],
    description="Rolling repricing / liquidity features for Kalshi.",
)
def silver_kalshi_event_repricing_features(
    context,
    silver_kalshi_market_snapshots: dict[str, str],
    silver_kalshi_trades: dict[str, str],
    silver_kalshi_markets: dict[str, str],
) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    bucket = silver_kalshi_market_snapshots.get("lake_bucket")
    snap = read_parquet_df(
        silver_kalshi_market_snapshots["silver_kalshi_market_snapshots"], bucket=bucket
    )
    tr = read_parquet_df(silver_kalshi_trades["silver_kalshi_trades"], bucket=bucket)
    mk = read_parquet_df(silver_kalshi_markets["silver_kalshi_markets"], bucket=bucket)
    df = build_event_repricing_features(snap, tr, mk)
    lineage = {**silver_kalshi_market_snapshots, **silver_kalshi_trades, **silver_kalshi_markets}
    df = _with_lineage(df, lineage)
    SILVER_KALSHI_EVENT_REPRICING_FEATURES.validate(df)
    key = _canonical_partition_output_key(
        layer="silver",
        dataset="kalshi_event_repricing_features",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(df, key, bucket=bucket)
    return {"silver_kalshi_event_repricing_features": key, "lake_bucket": bucket} if bucket else {
        "silver_kalshi_event_repricing_features": key
    }


@asset(
    partitions_def=hourly_partitions_def,
    deps=[bronze_polymarket],
    description="Build silver belief_price_snapshots from bronze Polymarket prices.",
)
def silver_belief_price_snapshots(context, bronze_polymarket: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    bucket = bronze_polymarket.get("lake_bucket")
    prices_df = read_parquet_df(bronze_polymarket["bronze_polymarket_prices"], bucket=bucket)
    belief_df = to_belief_price_snapshots(prices_df)
    belief_df = _with_lineage(belief_df, bronze_polymarket)
    SILVER_BELIEF_PRICE_SNAPSHOTS.validate(belief_df)
    key = _canonical_partition_output_key(
        layer="silver",
        dataset="belief_price_snapshots",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(belief_df, key, bucket=bucket)

    written = (
        {"silver_belief_price_snapshots": key, "lake_bucket": bucket}
        if bucket
        else {"silver_belief_price_snapshots": key}
    )
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(belief_df, "height", 0)),
            "rows_before_dedupe": int(getattr(prices_df, "height", 0)),
            "rows_after_dedupe": int(getattr(belief_df, "height", 0)),
            "dup": MetadataValue.json(
                _dup_metrics(belief_df, ["timestamp", "market_id", "outcome"])
            ),
        }
    )
    return written


@asset(
    partitions_def=hourly_partitions_def,
    deps=[bronze_binance],
    description="Build silver crypto_candles_1m from bronze Binance klines.",
)
def silver_crypto_candles_1m(context, bronze_binance: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    bucket = bronze_binance.get("lake_bucket")
    klines_df = read_parquet_df(bronze_binance["bronze_binance_klines"], bucket=bucket)
    candles_df = to_crypto_candles_1m(klines_df)
    candles_df = _with_lineage(candles_df, bronze_binance)
    SILVER_CRYPTO_CANDLES_1M.validate(candles_df)

    key = _canonical_partition_output_key(
        layer="silver",
        dataset="crypto_candles_1m",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(candles_df, key, bucket=bucket)

    written = (
        {"silver_crypto_candles_1m": key, "lake_bucket": bucket}
        if bucket
        else {"silver_crypto_candles_1m": key}
    )
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(candles_df, "height", 0)),
            "rows_before_dedupe": int(getattr(klines_df, "height", 0)),
            "rows_after_dedupe": int(getattr(candles_df, "height", 0)),
            "dup": MetadataValue.json(_dup_metrics(candles_df, ["timestamp", "asset"])),
        }
    )
    return written


@asset(
    partitions_def=hourly_partitions_def,
    deps=[bronze_gdelt],
    description="Build silver narrative_counts from bronze GDELT timeline series (may be empty).",
)
def silver_narrative_counts(context, bronze_gdelt: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    bucket = bronze_gdelt.get("lake_bucket")
    timeline_df = read_parquet_df(bronze_gdelt["bronze_gdelt_timeline"], bucket=bucket)
    counts_df = to_narrative_counts(timeline_df)
    counts_df = _with_lineage(counts_df, bronze_gdelt)
    SILVER_NARRATIVE_COUNTS.validate(counts_df)

    key = _canonical_partition_output_key(
        layer="silver",
        dataset="narrative_counts",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(counts_df, key, bucket=bucket)

    written = (
        {"silver_narrative_counts": key, "lake_bucket": bucket}
        if bucket
        else {"silver_narrative_counts": key}
    )
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(counts_df, "height", 0)),
            "rows_before_dedupe": int(getattr(timeline_df, "height", 0)),
            "rows_after_dedupe": int(getattr(counts_df, "height", 0)),
            "dup": MetadataValue.json(
                _dup_metrics(counts_df, ["timestamp", "narrative", "query", "source"])
            ),
        }
    )
    return written


@asset(
    partitions_def=hourly_partitions_def,
    deps=[raw_fear_greed],
    description="Normalize Alternative.me Fear & Greed raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_fear_greed(context) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    if partition_window is None:
        raise ValueError("bronze_fear_greed requires an hourly partition window")
    partition_key = partition_window.partition_key
    partition_start = partition_window.start
    partition_end = partition_window.end

    candidates, read_bucket = list_raw_fear_greed_for_partition_window(
        partition_start=partition_start,
        partition_end=partition_end,
        include_canonical_hourly=True,
    )
    chosen_keys = [it["raw_fear_greed_payload"] for it in candidates]
    raw_records = _read_records_for_keys(chosen_keys, bucket=read_bucket)
    last_batch_id = candidates[-1]["source_batch_id"] if candidates else ""
    lineage = {
        "source_batch_id": last_batch_id,
        "source_window_start": partition_start.isoformat(),
        "source_window_end": partition_end.isoformat(),
    }

    # Guardrail: never contaminate normalized outputs with API-declared errors.
    error_records = [r for r in raw_records if r.get("ok") is False and r.get("error")]
    if error_records:
        raise ValueError(f"Fear & Greed raw contains error records: sample={error_records[:1]}")

    bronze_norm = normalize_fear_greed_payload_records(raw_records)
    BRONZE_FEAR_GREED.validate(bronze_norm)
    bronze_df = _with_lineage(bronze_norm, lineage)

    key = _canonical_partition_output_key(
        layer="bronze",
        dataset="provider=alternative_me_fear_greed",
        run_date=run_date,
        partition_key=partition_key,
    )
    write_parquet_df(bronze_df, key, bucket=read_bucket)

    written = {
        "bronze_fear_greed": key,
        "source_batch_id": last_batch_id,
        "source_window_start": partition_start.isoformat(),
        "source_window_end": partition_end.isoformat(),
        "lake_bucket": read_bucket,
    }
    processed_keys = sorted(set(chosen_keys))
    write_processing_watermark(
        build_watermark(
            consumer_asset="bronze_fear_greed",
            source="alternative_me_fear_greed",
            partition_key=partition_key,
            partition_start=partition_start,
            partition_end=partition_end,
            processed_input_keys=processed_keys,
            last_processed_batch_id=last_batch_id or None,
        ),
        bucket=read_bucket,
    )
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(bronze_df, "height", 0)),
            "window_start_utc": partition_start.isoformat(),
            "window_end_utc": partition_end.isoformat(),
            "window_closed_open": "[start,end)",
            "input_microbatches_seen": len(candidates),
            "input_microbatches_processed": len(candidates),
            "dup": MetadataValue.json(_dup_metrics(bronze_df, ["source", "date_utc"])),
        }
    )
    return written


@asset(
    partitions_def=hourly_partitions_def,
    deps=[bronze_fear_greed],
    description="Build silver fear_greed_daily (daily values) from bronze Fear & Greed.",
)
def silver_fear_greed_daily(context, bronze_fear_greed: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    bucket = bronze_fear_greed.get("lake_bucket")
    bronze_df = read_parquet_df(bronze_fear_greed["bronze_fear_greed"], bucket=bucket)
    daily_df = to_fear_greed_daily(bronze_df)
    daily_df = _with_lineage(daily_df, bronze_fear_greed)
    SILVER_FEAR_GREED_DAILY.validate(daily_df)

    key = _canonical_partition_output_key(
        layer="silver",
        dataset="fear_greed_daily",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(daily_df, key, bucket=bucket)

    written = (
        {"silver_fear_greed_daily": key, "lake_bucket": bucket}
        if bucket
        else {"silver_fear_greed_daily": key}
    )
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(daily_df, "height", 0)),
            "dup": MetadataValue.json(_dup_metrics(daily_df, ["source", "date_utc"])),
        }
    )
    return written


@asset(
    partitions_def=hourly_partitions_def,
    deps=[silver_fear_greed_daily],
    description="Build silver fear_greed_regime_features from fear_greed_daily.",
)
def silver_fear_greed_regime_features(
    context, silver_fear_greed_daily: dict[str, str]
) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    bucket = silver_fear_greed_daily.get("lake_bucket")
    daily_df = read_parquet_df(silver_fear_greed_daily["silver_fear_greed_daily"], bucket=bucket)
    feats_df = build_regime_features(daily_df)
    feats_df = _with_lineage(feats_df, silver_fear_greed_daily)
    SILVER_FEAR_GREED_REGIME_FEATURES.validate(feats_df)

    key = _canonical_partition_output_key(
        layer="silver",
        dataset="fear_greed_regime_features",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(feats_df, key, bucket=bucket)

    written = (
        {"silver_fear_greed_regime_features": key, "lake_bucket": bucket}
        if bucket
        else {"silver_fear_greed_regime_features": key}
    )
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(feats_df, "height", 0)),
        }
    )
    return written


__all__ = [
    "bronze_binance",
    "bronze_fear_greed",
    "bronze_gdelt",
    "bronze_kalshi",
    "bronze_polymarket",
    "silver_belief_price_snapshots",
    "silver_crypto_candles_1m",
    "silver_fear_greed_daily",
    "silver_fear_greed_regime_features",
    "silver_kalshi_candlesticks",
    "silver_kalshi_event_repricing_features",
    "silver_kalshi_events",
    "silver_kalshi_market_snapshots",
    "silver_kalshi_markets",
    "silver_kalshi_orderbook_snapshots",
    "silver_kalshi_series",
    "silver_kalshi_trades",
    "silver_narrative_counts",
]
