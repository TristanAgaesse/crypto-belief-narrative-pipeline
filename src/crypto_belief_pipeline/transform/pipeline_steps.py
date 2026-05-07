"""Per-source raw-records -> bronze + silver write steps.

Both the live and sample pipeline runners share the same shape for each
provider:

1. start from a list of raw records (loaded from the lake or from bundled
   sample fixtures),
2. normalize into a typed bronze parquet,
3. derive the source-specific silver contract table,
4. write both layers.

The only differences between live and sample are *where* the raw records come
from and which bucket the parquet writes target. Centralizing the per-source
graph here keeps the runners thin and prevents drift between them.
"""

from __future__ import annotations

from datetime import date

from crypto_belief_pipeline.contracts import (
    SILVER_BELIEF_PRICE_SNAPSHOTS,
    SILVER_CRYPTO_CANDLES_1M,
    SILVER_NARRATIVE_COUNTS,
)
from crypto_belief_pipeline.lake.paths import partition_path
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


def normalize_polymarket_to_silver(
    *,
    run_date: date | str,
    markets_records: list[dict],
    prices_records: list[dict],
    bucket: str | None = None,
) -> dict[str, str]:
    """Write bronze Polymarket markets + prices and silver belief_price_snapshots.

    Returns a dict of dataset_name -> on-bucket key:
    - ``bronze_polymarket_markets``
    - ``bronze_polymarket_prices``
    - ``silver_belief_price_snapshots``
    """

    bronze_pm = partition_path("bronze", "provider=polymarket", run_date)
    silver_belief_prefix = partition_path("silver", "belief_price_snapshots", run_date)

    bronze_markets = normalize_markets(markets_records)
    markets_key = _k(bronze_pm, "markets.parquet")
    write_parquet_df(bronze_markets, markets_key, bucket=bucket)

    bronze_prices = normalize_price_snapshots(prices_records)
    prices_key = _k(bronze_pm, "prices.parquet")
    write_parquet_df(bronze_prices, prices_key, bucket=bucket)

    silver_belief = to_belief_price_snapshots(bronze_prices)
    # Fail fast on contract violations *before* writing — a bad silver should
    # never reach the lake where downstream gold/DQ would silently consume it.
    SILVER_BELIEF_PRICE_SNAPSHOTS.validate(silver_belief)
    belief_key = _k(silver_belief_prefix, "data.parquet")
    write_parquet_df(silver_belief, belief_key, bucket=bucket)

    return {
        "bronze_polymarket_markets": markets_key,
        "bronze_polymarket_prices": prices_key,
        "silver_belief_price_snapshots": belief_key,
    }


def normalize_binance_to_silver(
    *,
    run_date: date | str,
    klines_records: list[dict],
    bucket: str | None = None,
) -> dict[str, str]:
    """Write bronze Binance klines and silver crypto_candles_1m.

    Returns a dict of dataset_name -> on-bucket key:
    - ``bronze_binance_klines``
    - ``silver_crypto_candles_1m``
    """

    bronze_bn = partition_path("bronze", "provider=binance", run_date)
    silver_candles_prefix = partition_path("silver", "crypto_candles_1m", run_date)

    bronze_klines = normalize_klines(klines_records)
    klines_key = _k(bronze_bn, "klines.parquet")
    write_parquet_df(bronze_klines, klines_key, bucket=bucket)

    silver_candles = to_crypto_candles_1m(bronze_klines)
    SILVER_CRYPTO_CANDLES_1M.validate(silver_candles)
    candles_key = _k(silver_candles_prefix, "data.parquet")
    write_parquet_df(silver_candles, candles_key, bucket=bucket)

    return {
        "bronze_binance_klines": klines_key,
        "silver_crypto_candles_1m": candles_key,
    }


def normalize_gdelt_to_silver(
    *,
    run_date: date | str,
    timeline_records: list[dict],
    bucket: str | None = None,
) -> dict[str, str]:
    """Write bronze GDELT timeline and silver narrative_counts.

    Returns a dict of dataset_name -> on-bucket key:
    - ``bronze_gdelt_timeline``
    - ``silver_narrative_counts``
    """

    bronze_gd = partition_path("bronze", "provider=gdelt", run_date)
    silver_counts_prefix = partition_path("silver", "narrative_counts", run_date)

    bronze_timeline = normalize_timeline(timeline_records)
    timeline_key = _k(bronze_gd, "timeline.parquet")
    write_parquet_df(bronze_timeline, timeline_key, bucket=bucket)

    silver_counts = to_narrative_counts(bronze_timeline)
    SILVER_NARRATIVE_COUNTS.validate(silver_counts)
    counts_key = _k(silver_counts_prefix, "data.parquet")
    write_parquet_df(silver_counts, counts_key, bucket=bucket)

    return {
        "bronze_gdelt_timeline": timeline_key,
        "silver_narrative_counts": counts_key,
    }


__all__ = [
    "normalize_binance_to_silver",
    "normalize_gdelt_to_silver",
    "normalize_polymarket_to_silver",
]
