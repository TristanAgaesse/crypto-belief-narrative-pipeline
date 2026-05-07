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
from crypto_belief_pipeline.lake.batches import generate_batch_id, split_batch_parts
from crypto_belief_pipeline.lake.paths import microbatch_key
from crypto_belief_pipeline.lake.read import read_parquet_df
from crypto_belief_pipeline.lake.write import write_parquet_df
from crypto_belief_pipeline.orchestration._helpers import (
    _dup_metrics,
    _partition_tick_now_utc,
    _safe_read_jsonl,
    _with_lineage,
    partitions_def,
)
from crypto_belief_pipeline.orchestration.assets_raw import (
    raw_binance,
    raw_gdelt,
    raw_polymarket,
)
from crypto_belief_pipeline.orchestration.raw_inputs_from_lake import (
    resolve_raw_binance_for_partition,
    resolve_raw_gdelt_for_partition,
    resolve_raw_polymarket_for_partition,
)
from crypto_belief_pipeline.orchestration.resources import resolve_run_date_from_context
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


@asset(
    partitions_def=partitions_def,
    deps=[raw_polymarket],
    description="Normalize Polymarket raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_polymarket(context) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    raw_polymarket, read_bucket = resolve_raw_polymarket_for_partition(run_date)
    markets_key = raw_polymarket["raw_polymarket_markets"]
    prices_key = raw_polymarket["raw_polymarket_prices"]
    source_batch_id = raw_polymarket.get("source_batch_id") or ""
    source_window_start = raw_polymarket.get("source_window_start") or ""
    source_window_end = raw_polymarket.get("source_window_end") or ""
    markets = _safe_read_jsonl(markets_key, bucket=read_bucket)
    prices = _safe_read_jsonl(prices_key, bucket=read_bucket)

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
            "dup_prices": MetadataValue.json(
                _dup_metrics(prices_df, ["timestamp", "market_id", "outcome"])
            ),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    deps=[raw_binance],
    description="Normalize Binance raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_binance(context) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    raw_binance, read_bucket = resolve_raw_binance_for_partition(run_date)
    chosen = raw_binance["raw_binance_klines"]
    source_batch_id = raw_binance.get("source_batch_id") or ""
    source_window_start = raw_binance.get("source_window_start") or ""
    source_window_end = raw_binance.get("source_window_end") or ""
    klines = _safe_read_jsonl(chosen, bucket=read_bucket)
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
def bronze_gdelt(context) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    raw_gdelt, read_bucket = resolve_raw_gdelt_for_partition(run_date)
    chosen = raw_gdelt["raw_gdelt_timeline"]
    source_batch_id = raw_gdelt.get("source_batch_id") or ""
    source_window_start = raw_gdelt.get("source_window_start") or ""
    source_window_end = raw_gdelt.get("source_window_end") or ""
    timeline = _safe_read_jsonl(chosen, bucket=read_bucket)
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
            "dup": MetadataValue.json(
                _dup_metrics(bronze_df, ["timestamp", "narrative", "query", "source"])
            ),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    deps=[bronze_polymarket],
    description="Build silver belief_price_snapshots from bronze Polymarket prices.",
)
def silver_belief_price_snapshots(context, bronze_polymarket: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    prices_df = read_parquet_df(bronze_polymarket["bronze_polymarket_prices"])
    belief_df = to_belief_price_snapshots(prices_df)
    belief_df = _with_lineage(belief_df, bronze_polymarket)
    SILVER_BELIEF_PRICE_SNAPSHOTS.validate(belief_df)

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
            "dup": MetadataValue.json(
                _dup_metrics(belief_df, ["timestamp", "market_id", "outcome"])
            ),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    deps=[bronze_binance],
    description="Build silver crypto_candles_1m from bronze Binance klines.",
)
def silver_crypto_candles_1m(context, bronze_binance: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date_from_context(context)
    klines_df = read_parquet_df(bronze_binance["bronze_binance_klines"])
    candles_df = to_crypto_candles_1m(klines_df)
    candles_df = _with_lineage(candles_df, bronze_binance)
    SILVER_CRYPTO_CANDLES_1M.validate(candles_df)

    batch_id = generate_batch_id(_partition_tick_now_utc(run_date))
    parts = split_batch_parts(batch_id)
    key = microbatch_key(
        "silver", "crypto_candles_1m", parts.date, parts.hour, batch_id, ".parquet"
    )
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
    run_date = resolve_run_date_from_context(context)
    timeline_df = read_parquet_df(bronze_gdelt["bronze_gdelt_timeline"])
    counts_df = to_narrative_counts(timeline_df)
    counts_df = _with_lineage(counts_df, bronze_gdelt)
    SILVER_NARRATIVE_COUNTS.validate(counts_df)

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
