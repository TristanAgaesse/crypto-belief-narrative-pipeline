from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class DatasetContract:
    name: str
    required_columns: frozenset[str]

    def validate(self, df: pl.DataFrame) -> None:
        missing = sorted(self.required_columns - set(df.columns))
        if missing:
            raise ValueError(f"{self.name}: missing required columns: {missing}")


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
)

SILVER_NARRATIVE_COUNTS = DatasetContract(
    name="silver_narrative_counts",
    required_columns=frozenset(
        {
            "timestamp",
            "narrative",
            "mention_volume",
        }
    ),
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
)

__all__ = [
    "DatasetContract",
    "SILVER_BELIEF_PRICE_SNAPSHOTS",
    "SILVER_CRYPTO_CANDLES_1M",
    "SILVER_NARRATIVE_COUNTS",
    "GOLD_TRAINING_EXAMPLES",
    "GOLD_LIVE_SIGNALS",
]
