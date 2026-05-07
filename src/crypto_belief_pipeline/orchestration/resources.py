from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime


def resolve_run_date(partition_key: str | None) -> date:
    """Resolve a run date from a Dagster partition key or default to today's UTC date."""

    if partition_key:
        return date.fromisoformat(partition_key)
    return datetime.now(UTC).date()


@dataclass(frozen=True)
class LakeKeys:
    """Standard lake keys emitted by the pipeline for one run_date partition."""

    raw_polymarket_markets: str | None = None
    raw_polymarket_prices: str | None = None
    raw_binance_klines: str | None = None
    raw_gdelt_timeline: str | None = None

    bronze_polymarket_markets: str | None = None
    bronze_polymarket_prices: str | None = None
    bronze_binance_klines: str | None = None
    bronze_gdelt_timeline: str | None = None

    silver_belief_price_snapshots: str | None = None
    silver_crypto_candles_1m: str | None = None
    silver_narrative_counts: str | None = None

    gold_training_examples: str | None = None
    gold_alpha_events: str | None = None
