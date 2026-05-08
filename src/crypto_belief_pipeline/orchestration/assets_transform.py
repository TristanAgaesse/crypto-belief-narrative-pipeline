"""Bronze + silver Dagster assets: typed normalize + research-ready joins.

Bronze writes typed Parquet from raw JSONL. Silver derives source-aware
contract tables (belief_price_snapshots, crypto_candles_1m, narrative_counts).
"""

from __future__ import annotations

from crypto_belief_pipeline.contracts import (
    SILVER_BELIEF_PRICE_SNAPSHOTS,
    SILVER_CRYPTO_CANDLES_1M,
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
    raw_gdelt,
    raw_polymarket,
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


@asset(
    partitions_def=hourly_partitions_def,
    deps=[raw_polymarket],
    description="Normalize Polymarket raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_polymarket(context, raw_polymarket: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    if partition_window is None:
        raise ValueError("bronze_polymarket requires an hourly partition window")
    partition_key = partition_window.partition_key
    partition_start = partition_window.start
    partition_end = partition_window.end

    markets_key = raw_polymarket["raw_polymarket_markets"]
    prices_key = raw_polymarket["raw_polymarket_prices"]
    markets = _safe_read_jsonl(markets_key)
    prices = _safe_read_jsonl(prices_key)
    markets_df = _with_lineage(normalize_markets(markets), raw_polymarket)
    prices_df = _with_lineage(normalize_price_snapshots(prices), raw_polymarket)
    last_batch_id = raw_polymarket.get("source_batch_id") or ""

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
    write_parquet_df(markets_df, out_markets_key)
    write_parquet_df(prices_df, out_prices_key)

    written = {
        "bronze_polymarket_markets": out_markets_key,
        "bronze_polymarket_prices": out_prices_key,
        "source_batch_id": last_batch_id,
        "source_window_start": partition_start.isoformat(),
        "source_window_end": partition_end.isoformat(),
    }
    processed_keys = [markets_key, prices_key]
    write_processing_watermark(
        build_watermark(
            consumer_asset="bronze_polymarket",
            source="polymarket",
            partition_key=partition_key,
            partition_start=partition_start,
            partition_end=partition_end,
            processed_input_keys=processed_keys,
            last_processed_batch_id=last_batch_id or None,
        )
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
            "input_microbatches_seen": 1,
            "input_microbatches_processed": 1,
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
def bronze_binance(context, raw_binance: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    if partition_window is None:
        raise ValueError("bronze_binance requires an hourly partition window")
    partition_key = partition_window.partition_key
    partition_start = partition_window.start
    partition_end = partition_window.end

    chosen = raw_binance["raw_binance_klines"]
    klines = _safe_read_jsonl(chosen)
    bronze_df = _with_lineage(normalize_klines(klines), raw_binance)
    last_batch_id = raw_binance.get("source_batch_id") or ""
    key = _canonical_partition_output_key(
        layer="bronze",
        dataset="provider=binance",
        run_date=run_date,
        partition_key=partition_key,
    )
    write_parquet_df(bronze_df, key)

    written = {
        "bronze_binance_klines": key,
        "source_batch_id": last_batch_id,
        "source_window_start": partition_start.isoformat(),
        "source_window_end": partition_end.isoformat(),
    }
    processed_keys = [chosen]
    write_processing_watermark(
        build_watermark(
            consumer_asset="bronze_binance",
            source="binance",
            partition_key=partition_key,
            partition_start=partition_start,
            partition_end=partition_end,
            processed_input_keys=processed_keys,
            last_processed_batch_id=last_batch_id or None,
        )
    )
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(bronze_df, "height", 0)),
            "window_start_utc": partition_start.isoformat(),
            "window_end_utc": partition_end.isoformat(),
            "window_closed_open": "[start,end)",
            "input_microbatches_seen": 1,
            "input_microbatches_processed": 1,
            "dup": MetadataValue.json(_dup_metrics(bronze_df, ["timestamp", "symbol", "interval"])),
        }
    )
    return written


@asset(
    partitions_def=hourly_partitions_def,
    deps=[raw_gdelt],
    description="Normalize GDELT raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_gdelt(context, raw_gdelt: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    partition_window = resolve_partition_window_from_context(context)
    if partition_window is None:
        raise ValueError("bronze_gdelt requires an hourly partition window")
    partition_key = partition_window.partition_key
    partition_start = partition_window.start
    partition_end = partition_window.end

    chosen = raw_gdelt["raw_gdelt_timeline"]
    timeline = _safe_read_jsonl(chosen)
    bronze_df = _with_lineage(normalize_timeline(timeline), raw_gdelt)
    last_batch_id = raw_gdelt.get("source_batch_id") or ""
    key = _canonical_partition_output_key(
        layer="bronze",
        dataset="provider=gdelt",
        run_date=run_date,
        partition_key=partition_key,
    )
    write_parquet_df(bronze_df, key)

    written = {
        "bronze_gdelt_timeline": key,
        "source_batch_id": last_batch_id,
        "source_window_start": partition_start.isoformat(),
        "source_window_end": partition_end.isoformat(),
    }
    processed_keys = [chosen]
    write_processing_watermark(
        build_watermark(
            consumer_asset="bronze_gdelt",
            source="gdelt",
            partition_key=partition_key,
            partition_start=partition_start,
            partition_end=partition_end,
            processed_input_keys=processed_keys,
            last_processed_batch_id=last_batch_id or None,
        )
    )
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(bronze_df, "height", 0)),
            "window_start_utc": partition_start.isoformat(),
            "window_end_utc": partition_end.isoformat(),
            "window_closed_open": "[start,end)",
            "input_microbatches_seen": 1,
            "input_microbatches_processed": 1,
            "dup": MetadataValue.json(
                _dup_metrics(bronze_df, ["timestamp", "narrative", "query", "source"])
            ),
        }
    )
    return written


@asset(
    partitions_def=hourly_partitions_def,
    deps=[bronze_polymarket],
    description="Build silver belief_price_snapshots from bronze Polymarket prices.",
)
def silver_belief_price_snapshots(context, bronze_polymarket: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    prices_df = read_parquet_df(bronze_polymarket["bronze_polymarket_prices"])
    belief_df = to_belief_price_snapshots(prices_df)
    belief_df = _with_lineage(belief_df, bronze_polymarket)
    SILVER_BELIEF_PRICE_SNAPSHOTS.validate(belief_df)
    key = _canonical_partition_output_key(
        layer="silver",
        dataset="belief_price_snapshots",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(belief_df, key)

    written = {"silver_belief_price_snapshots": key}
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
    klines_df = read_parquet_df(bronze_binance["bronze_binance_klines"])
    candles_df = to_crypto_candles_1m(klines_df)
    candles_df = _with_lineage(candles_df, bronze_binance)
    SILVER_CRYPTO_CANDLES_1M.validate(candles_df)

    key = _canonical_partition_output_key(
        layer="silver",
        dataset="crypto_candles_1m",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(candles_df, key)

    written = {"silver_crypto_candles_1m": key}
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
    timeline_df = read_parquet_df(bronze_gdelt["bronze_gdelt_timeline"])
    counts_df = to_narrative_counts(timeline_df)
    counts_df = _with_lineage(counts_df, bronze_gdelt)
    SILVER_NARRATIVE_COUNTS.validate(counts_df)

    key = _canonical_partition_output_key(
        layer="silver",
        dataset="narrative_counts",
        run_date=run_date,
        partition_key=context.partition_key if context.has_partition_key else None,
    )
    write_parquet_df(counts_df, key)

    written = {"silver_narrative_counts": key}
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


__all__ = [
    "bronze_binance",
    "bronze_gdelt",
    "bronze_polymarket",
    "silver_belief_price_snapshots",
    "silver_crypto_candles_1m",
    "silver_narrative_counts",
]
