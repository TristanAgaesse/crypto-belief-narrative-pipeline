from __future__ import annotations

from datetime import date

from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.read import read_jsonl_records
from crypto_belief_pipeline.lake.write import write_parquet_df
from crypto_belief_pipeline.transform.normalize_binance import (
    normalize_klines,
    to_crypto_candles_1m,
)
from crypto_belief_pipeline.transform.normalize_gdelt import (
    normalize_timeline,
    to_narrative_counts,
)
from crypto_belief_pipeline.transform.normalize_polymarket import (
    normalize_markets,
    normalize_price_snapshots,
    to_belief_price_snapshots,
)


def _k(prefix: str, name: str) -> str:
    return f"{prefix}/{name}"


def _safe_read(key: str | None) -> list[dict]:
    if not key:
        return []
    try:
        return read_jsonl_records(key)
    except Exception:
        return []


def run_raw_to_silver(
    run_date: date | str,
    polymarket_markets_key: str | None,
    polymarket_prices_key: str | None,
    binance_klines_key: str | None,
    gdelt_timeline_key: str | None,
) -> dict[str, str]:
    """Read raw JSONL from S3, normalize to bronze and silver, and write Parquet.

    Reuses the existing normalizers, so the same code path applies to
    both sample data and live collector outputs (which share raw contracts).
    """

    pm_markets = _safe_read(polymarket_markets_key)
    pm_prices = _safe_read(polymarket_prices_key)
    bn_klines = _safe_read(binance_klines_key)
    gd_timeline = _safe_read(gdelt_timeline_key)

    bronze_pm = partition_path("bronze", "provider=polymarket", run_date)
    bronze_bn = partition_path("bronze", "provider=binance", run_date)
    bronze_gd = partition_path("bronze", "provider=gdelt", run_date)

    written: dict[str, str] = {}

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

    silver_belief = to_belief_price_snapshots(bronze_prices)
    written["silver_belief_price_snapshots"] = _k(
        partition_path("silver", "belief_price_snapshots", run_date),
        "data.parquet",
    )
    write_parquet_df(silver_belief, written["silver_belief_price_snapshots"])

    silver_candles = to_crypto_candles_1m(bronze_klines)
    written["silver_crypto_candles_1m"] = _k(
        partition_path("silver", "crypto_candles_1m", run_date),
        "data.parquet",
    )
    write_parquet_df(silver_candles, written["silver_crypto_candles_1m"])

    silver_counts = to_narrative_counts(bronze_timeline)
    written["silver_narrative_counts"] = _k(
        partition_path("silver", "narrative_counts", run_date),
        "data.parquet",
    )
    write_parquet_df(silver_counts, written["silver_narrative_counts"])

    return written
