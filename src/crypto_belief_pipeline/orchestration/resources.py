from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta


def resolve_run_date(partition_key: str | None) -> date:
    """Resolve a run date from common Dagster partition key formats."""

    if partition_key:
        # Daily: YYYY-MM-DD
        if len(partition_key) == 10 and partition_key[4] == "-" and partition_key[7] == "-":
            return date.fromisoformat(partition_key)
        # Minute / 5m: YYYY-MM-DDTHH:MM (UTC)
        if "T" in partition_key:
            try:
                return datetime.fromisoformat(partition_key).date()
            except ValueError:
                # Back-compat if an older run key included `%z` like +0000.
                return datetime.strptime(partition_key, "%Y-%m-%dT%H:%M%z").date()
        # Hourly: YYYY-MM-DD-HH:MM
        return datetime.strptime(partition_key, "%Y-%m-%d-%H:%M").date()
    return datetime.now(UTC).date()


def resolve_run_date_from_context(context) -> date:
    """Like :func:`resolve_run_date`, but safe when the run is not partition-scoped."""

    pk = context.partition_key if context.has_partition_key else None
    return resolve_run_date(pk)


@dataclass(frozen=True)
class PartitionWindow:
    partition_key: str
    start: datetime
    end: datetime


def resolve_partition_window_from_context(context) -> PartitionWindow | None:
    """Resolve the current partition window as `[start,end)` in UTC when available."""

    if not getattr(context, "has_partition_key", False):
        return None

    partition_key = context.partition_key
    # For time-window partitions Dagster provides a canonical range directly.
    if hasattr(context, "partition_time_window"):
        tw = context.partition_time_window
        st = tw.start if tw.start.tzinfo else tw.start.replace(tzinfo=UTC)
        et = tw.end if tw.end.tzinfo else tw.end.replace(tzinfo=UTC)
        return PartitionWindow(
            partition_key=partition_key,
            start=st.astimezone(UTC),
            end=et.astimezone(UTC),
        )

    # Daily fallback.
    run_date = resolve_run_date(partition_key)
    st = datetime(run_date.year, run_date.month, run_date.day, tzinfo=UTC)
    return PartitionWindow(partition_key=partition_key, start=st, end=st + timedelta(days=1))


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
