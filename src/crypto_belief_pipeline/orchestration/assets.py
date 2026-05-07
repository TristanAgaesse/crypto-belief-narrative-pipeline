"""Aggregate Dagster asset module.

The asset definitions live in focused submodules:

* :mod:`crypto_belief_pipeline.orchestration._helpers` — shared helpers + partitions def.
* :mod:`crypto_belief_pipeline.orchestration.assets_raw` — raw layer (sample + live collectors).
* :mod:`crypto_belief_pipeline.orchestration.assets_transform` — bronze + silver transforms.
* :mod:`crypto_belief_pipeline.orchestration.assets_quality` — gold + Soda DQ + issues + reports.

This file is intentionally thin: it re-exports the canonical asset names plus
the legacy private helpers so that existing imports
(:mod:`crypto_belief_pipeline.orchestration.definitions`, tests) keep working
without behavioral change.
"""

from __future__ import annotations

from crypto_belief_pipeline.orchestration._helpers import (
    _clamp_window_to_partition,
    _dup_metrics,
    _k,
    _partition_bounds_utc,
    _partition_tick_now_utc,
    _read_yaml_mapping,
    _safe_read_jsonl,
    _sample_guardrails,
    _with_lineage,
    partitions_def,
)
from crypto_belief_pipeline.orchestration.assets_quality import (
    data_issues,
    gold_tables,
    markdown_reports,
    soda_data_quality,
)
from crypto_belief_pipeline.orchestration.assets_raw import (
    raw_binance,
    raw_gdelt,
    raw_polymarket,
    raw_sample_inputs,
)
from crypto_belief_pipeline.orchestration.assets_transform import (
    bronze_binance,
    bronze_gdelt,
    bronze_polymarket,
    silver_belief_price_snapshots,
    silver_crypto_candles_1m,
    silver_narrative_counts,
)

ALL_ASSETS = [
    raw_sample_inputs,
    raw_polymarket,
    raw_binance,
    raw_gdelt,
    bronze_polymarket,
    bronze_binance,
    bronze_gdelt,
    silver_belief_price_snapshots,
    silver_crypto_candles_1m,
    silver_narrative_counts,
    gold_tables,
    soda_data_quality,
    data_issues,
    markdown_reports,
]


__all__ = [
    "ALL_ASSETS",
    "_clamp_window_to_partition",
    "_dup_metrics",
    "_k",
    "_partition_bounds_utc",
    "_partition_tick_now_utc",
    "_read_yaml_mapping",
    "_safe_read_jsonl",
    "_sample_guardrails",
    "_with_lineage",
    "bronze_binance",
    "bronze_gdelt",
    "bronze_polymarket",
    "data_issues",
    "gold_tables",
    "markdown_reports",
    "partitions_def",
    "raw_binance",
    "raw_gdelt",
    "raw_polymarket",
    "raw_sample_inputs",
    "silver_belief_price_snapshots",
    "silver_crypto_candles_1m",
    "silver_narrative_counts",
    "soda_data_quality",
]
