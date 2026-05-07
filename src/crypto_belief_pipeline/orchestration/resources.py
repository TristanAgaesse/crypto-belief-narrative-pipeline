from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime


def resolve_run_date(partition_key: str | None) -> date:
    """Resolve a run date from a Dagster partition key or default to today's UTC date."""

    if partition_key:
        return date.fromisoformat(partition_key)
    return datetime.now(UTC).date()


def resolve_run_date_from_context(context) -> date:
    """Like :func:`resolve_run_date`, but safe when the run is not partition-scoped."""

    pk = context.partition_key if context.has_partition_key else None
    return resolve_run_date(pk)


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
    gold_live_signals: str | None = None
