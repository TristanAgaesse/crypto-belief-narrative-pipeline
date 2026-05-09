from __future__ import annotations

from collections.abc import Iterable
from datetime import date

import polars as pl

from crypto_belief_pipeline.io_guardrails import resolve_sample_bucket
from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.write import write_jsonl_records
from crypto_belief_pipeline.sample_data import load_sample_jsonl
from crypto_belief_pipeline.transform import pipeline_steps as ps
from crypto_belief_pipeline.transform.pipeline_steps import (
    normalize_binance_to_silver,
    normalize_gdelt_to_silver,
    normalize_polymarket_to_silver,
)


def _k(prefix: str, name: str) -> str:
    return f"{prefix}/{name}"


def _stable_items(d: dict[str, str]) -> Iterable[tuple[str, str]]:
    for k in sorted(d.keys()):
        yield k, d[k]


def run_sample_pipeline(run_date: date | str = "2026-05-06") -> dict[str, str]:
    """Run the sample pipeline (raw -> bronze -> silver) into the sample bucket.

    Returns a mapping of dataset name -> on-bucket key (canonical layout, no
    sample-specific key prefix). The dedicated bucket is reported under the
    ``sample_bucket`` entry so callers can route downstream stages to the same
    bucket.
    """

    sample_bucket = resolve_sample_bucket()

    pm_markets = load_sample_jsonl("polymarket_markets_sample.jsonl")
    pm_prices = load_sample_jsonl("polymarket_prices_sample.jsonl")
    bn_klines = load_sample_jsonl("binance_klines_sample.jsonl")
    gd_timeline = load_sample_jsonl("gdelt_timeline_sample.jsonl")

    written: dict[str, str] = {}

    # 1) Raw: copy the bundled sample JSONL into the lake under canonical keys.
    raw_pm = partition_path("raw", "provider=polymarket", run_date)
    raw_bn = partition_path("raw", "provider=binance", run_date)
    raw_gd = partition_path("raw", "provider=gdelt", run_date)

    written["raw_polymarket_markets"] = _k(raw_pm, "sample_markets.jsonl")
    write_jsonl_records(pm_markets, written["raw_polymarket_markets"], bucket=sample_bucket)

    written["raw_polymarket_prices"] = _k(raw_pm, "sample_prices.jsonl")
    write_jsonl_records(pm_prices, written["raw_polymarket_prices"], bucket=sample_bucket)

    written["raw_binance_klines"] = _k(raw_bn, "sample_klines.jsonl")
    write_jsonl_records(bn_klines, written["raw_binance_klines"], bucket=sample_bucket)

    written["raw_gdelt_timeline"] = _k(raw_gd, "sample_timeline.jsonl")
    write_jsonl_records(gd_timeline, written["raw_gdelt_timeline"], bucket=sample_bucket)

    # 2+3) Bronze + silver: same per-source steps as the live pipeline, but
    # routed to the dedicated sample bucket instead of the live S3_BUCKET.
    written.update(
        normalize_polymarket_to_silver(
            run_date=run_date,
            markets_records=pm_markets,
            prices_records=pm_prices,
            bucket=sample_bucket,
        )
    )
    written.update(
        normalize_binance_to_silver(
            run_date=run_date,
            klines_records=bn_klines,
            bucket=sample_bucket,
        )
    )
    written.update(
        normalize_gdelt_to_silver(
            run_date=run_date,
            timeline_records=gd_timeline,
            bucket=sample_bucket,
        )
    )

    # Fear & Greed is not part of the bundled JSONL sample set, but gold/DQ expect
    # the silver dataset to exist to avoid reading stale partitions. Write an
    # explicit empty partition in sample mode.
    fg_daily_key = f"{partition_path('silver', 'fear_greed_daily', run_date)}/data.parquet"
    fg_regime_key = (
        f"{partition_path('silver', 'fear_greed_regime_features', run_date)}/data.parquet"
    )
    ps.write_parquet_df(
        pl.DataFrame(
            schema={
                "source": pl.String,
                "date_utc": pl.Date,
                "value": pl.Int64,
                "value_classification": pl.String,
                "processed_at": pl.Datetime(time_zone="UTC"),
            }
        ),
        fg_daily_key,
        bucket=sample_bucket,
    )
    ps.write_parquet_df(
        pl.DataFrame(
            schema={
                "source": pl.String,
                "date_utc": pl.Date,
                "value": pl.Int64,
                "risk_on_score": pl.Float64,
                "processed_at": pl.Datetime(time_zone="UTC"),
            }
        ),
        fg_regime_key,
        bucket=sample_bucket,
    )
    written["silver_fear_greed_daily"] = fg_daily_key
    written["silver_fear_greed_regime_features"] = fg_regime_key

    written["sample_bucket"] = sample_bucket
    return dict(_stable_items(written))
