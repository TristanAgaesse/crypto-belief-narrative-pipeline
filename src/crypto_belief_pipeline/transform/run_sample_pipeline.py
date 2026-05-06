from __future__ import annotations

from collections.abc import Iterable
from datetime import date

from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.write import write_jsonl_records, write_parquet_df
from crypto_belief_pipeline.sample_data import load_sample_jsonl
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


def _k(prefix: str, name: str) -> str:
    return f"{prefix}/{name}"


def _stable_items(d: dict[str, str]) -> Iterable[tuple[str, str]]:
    for k in sorted(d.keys()):
        yield k, d[k]


def run_sample_pipeline(run_date: date | str = "2026-05-06") -> dict[str, str]:
    # Load sample inputs
    pm_markets = load_sample_jsonl("polymarket_markets_sample.jsonl")
    pm_prices = load_sample_jsonl("polymarket_prices_sample.jsonl")
    bn_klines = load_sample_jsonl("binance_klines_sample.jsonl")
    gd_timeline = load_sample_jsonl("gdelt_timeline_sample.jsonl")

    written: dict[str, str] = {}

    # Raw
    raw_pm = partition_path("raw", "provider=polymarket", run_date)
    raw_bn = partition_path("raw", "provider=binance", run_date)
    raw_gd = partition_path("raw", "provider=gdelt", run_date)

    written["raw_polymarket_markets"] = _k(raw_pm, "sample_markets.jsonl")
    write_jsonl_records(pm_markets, written["raw_polymarket_markets"])

    written["raw_polymarket_prices"] = _k(raw_pm, "sample_prices.jsonl")
    write_jsonl_records(pm_prices, written["raw_polymarket_prices"])

    written["raw_binance_klines"] = _k(raw_bn, "sample_klines.jsonl")
    write_jsonl_records(bn_klines, written["raw_binance_klines"])

    written["raw_gdelt_timeline"] = _k(raw_gd, "sample_timeline.jsonl")
    write_jsonl_records(gd_timeline, written["raw_gdelt_timeline"])

    # Bronze
    bronze_pm = partition_path("bronze", "provider=polymarket", run_date)
    bronze_bn = partition_path("bronze", "provider=binance", run_date)
    bronze_gd = partition_path("bronze", "provider=gdelt", run_date)

    bronze_markets = normalize_markets(pm_markets)
    written["bronze_polymarket_markets"] = _k(bronze_pm, "markets.parquet")
    write_parquet_df(bronze_markets, written["bronze_polymarket_markets"])

    bronze_prices = normalize_price_snapshots(pm_prices)
    written["bronze_polymarket_prices"] = _k(bronze_pm, "prices.parquet")
    write_parquet_df(bronze_prices, written["bronze_polymarket_prices"])

    bronze_klines = normalize_klines(bn_klines)
    written["bronze_binance_klines"] = _k(bronze_bn, "klines.parquet")
    write_parquet_df(bronze_klines, written["bronze_binance_klines"])

    bronze_timeline = normalize_timeline(gd_timeline)
    written["bronze_gdelt_timeline"] = _k(bronze_gd, "timeline.parquet")
    write_parquet_df(bronze_timeline, written["bronze_gdelt_timeline"])

    # Silver
    silver_belief = to_belief_price_snapshots(bronze_prices)
    silver_belief_prefix = partition_path("silver", "belief_price_snapshots", run_date)
    written["silver_belief_price_snapshots"] = _k(silver_belief_prefix, "data.parquet")
    write_parquet_df(silver_belief, written["silver_belief_price_snapshots"])

    silver_candles = to_crypto_candles_1m(bronze_klines)
    silver_candles_prefix = partition_path("silver", "crypto_candles_1m", run_date)
    written["silver_crypto_candles_1m"] = _k(silver_candles_prefix, "data.parquet")
    write_parquet_df(silver_candles, written["silver_crypto_candles_1m"])

    silver_counts = to_narrative_counts(bronze_timeline)
    silver_counts_prefix = partition_path("silver", "narrative_counts", run_date)
    written["silver_narrative_counts"] = _k(silver_counts_prefix, "data.parquet")
    write_parquet_df(silver_counts, written["silver_narrative_counts"])

    return dict(_stable_items(written))
