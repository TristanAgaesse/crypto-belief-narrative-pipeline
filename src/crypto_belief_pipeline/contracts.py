from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl


@dataclass(frozen=True)
class DatasetContract:
    name: str
    required_columns: frozenset[str]
    dtypes: dict[str, Any] | None = None
    non_null_columns: frozenset[str] = frozenset()
    unique_key: tuple[str, ...] | None = None
    allowed_values: dict[str, frozenset[Any]] | None = None

    def validate(self, df: pl.DataFrame) -> None:
        missing = sorted(self.required_columns - set(df.columns))
        if missing:
            raise ValueError(f"{self.name}: missing required columns: {missing}")
        self._validate_dtypes(df)
        self._validate_non_null(df)
        self._validate_unique(df)
        self._validate_allowed_values(df)

    def _validate_dtypes(self, df: pl.DataFrame) -> None:
        if not self.dtypes:
            return
        bad: list[str] = []
        for name, expected in self.dtypes.items():
            if name not in df.columns:
                continue
            actual = df.schema[name]
            if _dtype_matches(actual, expected):
                continue
            # Empty test/dry-run frames sometimes carry Null for otherwise typed
            # columns. Non-empty data must satisfy the declared dtype exactly.
            if df.height == 0 and actual == pl.Null:
                continue
            bad.append(f"{name}: expected {expected}, got {actual}")
        if bad:
            raise ValueError(f"{self.name}: invalid column dtypes: {bad}")

    def _validate_non_null(self, df: pl.DataFrame) -> None:
        if not self.non_null_columns or df.height == 0:
            return
        failures: list[str] = []
        for name in sorted(self.non_null_columns):
            if name not in df.columns:
                continue
            nulls = int(df.select(pl.col(name).is_null().sum()).item())
            if nulls:
                failures.append(f"{name} has {nulls} nulls")
        if failures:
            raise ValueError(f"{self.name}: non-null constraint failed: {failures}")

    def _validate_unique(self, df: pl.DataFrame) -> None:
        if not self.unique_key or df.height == 0:
            return
        missing_key_cols = [c for c in self.unique_key if c not in df.columns]
        if missing_key_cols:
            return
        unique_rows = df.unique(subset=list(self.unique_key)).height
        duplicates = df.height - unique_rows
        if duplicates:
            raise ValueError(
                f"{self.name}: unique key {self.unique_key} has {duplicates} duplicate rows"
            )

    def _validate_allowed_values(self, df: pl.DataFrame) -> None:
        if not self.allowed_values or df.height == 0:
            return
        failures: list[str] = []
        for name, allowed in self.allowed_values.items():
            if name not in df.columns:
                continue
            invalid = (
                df.filter(pl.col(name).is_not_null() & ~pl.col(name).is_in(list(allowed)))
                .select(name)
                .unique()
                .to_series()
                .to_list()
            )
            if invalid:
                failures.append(f"{name}: {invalid}")
        if failures:
            raise ValueError(f"{self.name}: invalid values: {failures}")


def _dtype_matches(actual: Any, expected: Any) -> bool:
    if actual == expected:
        return True
    if isinstance(actual, pl.Datetime) and isinstance(expected, pl.Datetime):
        return True
    if isinstance(actual, pl.Duration) and isinstance(expected, pl.Duration):
        return True
    return False


BRONZE_POLYMARKET_MARKETS = DatasetContract(
    name="bronze_polymarket_markets",
    required_columns=frozenset(
        {
            "market_id",
            "question",
            "slug",
            "category",
            "active",
            "closed",
            "end_date",
            "liquidity",
            "volume",
            "raw_json",
            "ingested_at",
            "load_timestamp",
        }
    ),
    dtypes={
        "market_id": pl.String,
        "question": pl.String,
        "slug": pl.String,
        "category": pl.String,
        "active": pl.Boolean,
        "closed": pl.Boolean,
        "end_date": pl.Datetime,
        "liquidity": pl.Float64,
        "volume": pl.Float64,
        "raw_json": pl.String,
        "ingested_at": pl.Datetime,
        "load_timestamp": pl.Datetime,
    },
    non_null_columns=frozenset({"market_id", "raw_json", "ingested_at", "load_timestamp"}),
    unique_key=("market_id",),
)

BRONZE_POLYMARKET_PRICES = DatasetContract(
    name="bronze_polymarket_prices",
    required_columns=frozenset(
        {
            "timestamp",
            "market_id",
            "outcome",
            "price",
            "best_bid",
            "best_ask",
            "spread",
            "liquidity",
            "volume",
            "raw_json",
            "ingested_at",
            "load_timestamp",
        }
    ),
    dtypes={
        "timestamp": pl.Datetime,
        "market_id": pl.String,
        "outcome": pl.String,
        "price": pl.Float64,
        "best_bid": pl.Float64,
        "best_ask": pl.Float64,
        "spread": pl.Float64,
        "liquidity": pl.Float64,
        "volume": pl.Float64,
        "raw_json": pl.String,
        "ingested_at": pl.Datetime,
        "load_timestamp": pl.Datetime,
    },
    non_null_columns=frozenset(
        {"timestamp", "market_id", "outcome", "price", "raw_json", "ingested_at", "load_timestamp"}
    ),
    unique_key=("timestamp", "market_id", "outcome"),
)

BRONZE_BINANCE_KLINES = DatasetContract(
    name="bronze_binance_klines",
    required_columns=frozenset(
        {
            "timestamp",
            "exchange",
            "symbol",
            "interval",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "number_of_trades",
            "raw_json",
            "ingested_at",
            "load_timestamp",
        }
    ),
    dtypes={
        "timestamp": pl.Datetime,
        "exchange": pl.String,
        "symbol": pl.String,
        "interval": pl.String,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Float64,
        "quote_volume": pl.Float64,
        "number_of_trades": pl.Int64,
        "raw_json": pl.String,
        "ingested_at": pl.Datetime,
        "load_timestamp": pl.Datetime,
    },
    non_null_columns=frozenset(
        {
            "timestamp",
            "exchange",
            "symbol",
            "interval",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "number_of_trades",
            "raw_json",
            "ingested_at",
            "load_timestamp",
        }
    ),
    unique_key=("timestamp", "symbol", "interval"),
    allowed_values={"exchange": frozenset({"binance_usdm"})},
)

BRONZE_GDELT_TIMELINE = DatasetContract(
    name="bronze_gdelt_timeline",
    required_columns=frozenset(
        {
            "timestamp",
            "narrative",
            "query",
            "mention_volume",
            "avg_tone",
            "source",
            "raw_json",
            "ingested_at",
            "load_timestamp",
        }
    ),
    dtypes={
        "timestamp": pl.Datetime,
        "narrative": pl.String,
        "query": pl.String,
        "mention_volume": pl.Float64,
        "avg_tone": pl.Float64,
        "source": pl.String,
        "raw_json": pl.String,
        "ingested_at": pl.Datetime,
        "load_timestamp": pl.Datetime,
    },
    non_null_columns=frozenset(
        {
            "timestamp",
            "narrative",
            "query",
            "mention_volume",
            "source",
            "raw_json",
            "ingested_at",
            "load_timestamp",
        }
    ),
    unique_key=("timestamp", "narrative", "query", "source"),
    allowed_values={"source": frozenset({"gdelt"})},
)

SILVER_BELIEF_PRICE_SNAPSHOTS = DatasetContract(
    name="silver_belief_price_snapshots",
    required_columns=frozenset(
        {
            "timestamp",
            "platform",
            "market_id",
            "outcome",
            "price",
        }
    ),
    dtypes={
        "timestamp": pl.Datetime,
        "platform": pl.String,
        "market_id": pl.String,
        "outcome": pl.String,
        "price": pl.Float64,
    },
    non_null_columns=frozenset({"timestamp", "platform", "market_id", "outcome", "price"}),
    unique_key=("timestamp", "platform", "market_id", "outcome"),
    allowed_values={"platform": frozenset({"polymarket"})},
)

SILVER_CRYPTO_CANDLES_1M = DatasetContract(
    name="silver_crypto_candles_1m",
    required_columns=frozenset(
        {
            "timestamp",
            "exchange",
            "asset",
            "close",
        }
    ),
    dtypes={
        "timestamp": pl.Datetime,
        "exchange": pl.String,
        "asset": pl.String,
        "close": pl.Float64,
    },
    non_null_columns=frozenset({"timestamp", "exchange", "asset", "close"}),
    unique_key=("timestamp", "exchange", "asset"),
    allowed_values={"exchange": frozenset({"binance_usdm"})},
)

SILVER_NARRATIVE_COUNTS = DatasetContract(
    name="silver_narrative_counts",
    required_columns=frozenset(
        {
            "timestamp",
            "narrative",
            "query",
            "mention_volume",
            "source",
        }
    ),
    dtypes={
        "timestamp": pl.Datetime,
        "narrative": pl.String,
        "query": pl.String,
        "mention_volume": pl.Float64,
        "source": pl.String,
    },
    non_null_columns=frozenset({"timestamp", "narrative", "query", "mention_volume", "source"}),
    unique_key=("timestamp", "narrative", "query", "source"),
    allowed_values={"source": frozenset({"gdelt"})},
)

GOLD_TRAINING_EXAMPLES = DatasetContract(
    name="gold_training_examples",
    required_columns=frozenset(
        {
            "event_time",
            "market_id",
            "asset",
            "narrative",
            "belief_shock_abs_1h",
            "underreaction_score",
            "is_candidate_event",
        }
    ),
    dtypes={
        "event_time": pl.Datetime,
        "market_id": pl.String,
        "asset": pl.String,
        "narrative": pl.String,
        "belief_shock_abs_1h": pl.Float64,
        "underreaction_score": pl.Float64,
        "is_candidate_event": pl.Boolean,
    },
    non_null_columns=frozenset(
        {"event_time", "market_id", "asset", "narrative", "is_candidate_event"}
    ),
    unique_key=("event_time", "market_id", "asset", "narrative"),
)

GOLD_LIVE_SIGNALS = DatasetContract(
    name="gold_live_signals",
    required_columns=frozenset(
        {
            "event_time",
            "market_id",
            "asset",
            "narrative",
            "is_candidate_event",
        }
    ),
    dtypes={
        "event_time": pl.Datetime,
        "market_id": pl.String,
        "asset": pl.String,
        "narrative": pl.String,
        "is_candidate_event": pl.Boolean,
    },
    non_null_columns=frozenset(
        {"event_time", "market_id", "asset", "narrative", "is_candidate_event"}
    ),
    unique_key=("event_time", "market_id", "asset", "narrative"),
    allowed_values={"is_candidate_event": frozenset({True})},
)

__all__ = [
    "BRONZE_BINANCE_KLINES",
    "BRONZE_GDELT_TIMELINE",
    "BRONZE_POLYMARKET_MARKETS",
    "BRONZE_POLYMARKET_PRICES",
    "DatasetContract",
    "SILVER_BELIEF_PRICE_SNAPSHOTS",
    "SILVER_CRYPTO_CANDLES_1M",
    "SILVER_NARRATIVE_COUNTS",
    "GOLD_TRAINING_EXAMPLES",
    "GOLD_LIVE_SIGNALS",
]
