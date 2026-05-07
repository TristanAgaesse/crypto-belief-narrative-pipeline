from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from crypto_belief_pipeline.collectors.binance import collect_binance_raw
from crypto_belief_pipeline.collectors.gdelt import collect_gdelt_raw
from crypto_belief_pipeline.collectors.polymarket import collect_polymarket_raw
from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.write import write_jsonl_records

_MARKETS_KEYWORDS_PATH = "config/markets_keywords.yaml"
_NARRATIVES_PATH = "config/narratives.yaml"


def _to_date(value: date | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError(f"Unsupported run_date type: {type(value).__name__}")


def _load_polymarket_config(path: str | Path = _MARKETS_KEYWORDS_PATH) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    section = data.get("polymarket") if isinstance(data, dict) else None
    return section if isinstance(section, dict) else {}


def _k(prefix: str, name: str) -> str:
    return f"{prefix}/{name}"


def run_live_collectors(
    run_date: date | str,
    collect_polymarket: bool = True,
    collect_binance: bool = True,
    collect_gdelt: bool = True,
    binance_limit: int = 120,
    polymarket_limit: int = 100,
    gdelt_start_date: str | None = None,
    gdelt_end_date: str | None = None,
    gdelt_min_interval_sec: float = 5.5,
    gdelt_max_attempts: int = 8,
    gdelt_timeout: float = 60.0,
    markets_keywords_path: str | Path = _MARKETS_KEYWORDS_PATH,
    narratives_config_path: str | Path = _NARRATIVES_PATH,
) -> dict[str, str]:
    """Run live collectors and write raw JSONL files to S3.

    Returns a mapping of dataset label to the S3 key written.
    """

    rd = _to_date(run_date)
    # Temporary windowing: use the full UTC day for this run_date.
    # Dagster will become the single window owner in the next step.
    start_time = datetime(rd.year, rd.month, rd.day, tzinfo=UTC)
    end_time = start_time + timedelta(days=1)

    written: dict[str, str] = {}

    raw_pm_prefix = partition_path("raw", "provider=polymarket", rd)
    raw_bn_prefix = partition_path("raw", "provider=binance", rd)
    raw_gd_prefix = partition_path("raw", "provider=gdelt", rd)

    if collect_polymarket:
        cfg = _load_polymarket_config(markets_keywords_path)
        keywords = cfg.get("keywords") or []
        active = bool(cfg.get("active", True))
        closed = bool(cfg.get("closed", False))
        markets, prices, _pm_meta = collect_polymarket_raw(
            limit=polymarket_limit,
            keywords=keywords,
            start_time=start_time,
            end_time=end_time,
            active=active,
            closed=closed,
        )
        markets_key = _k(raw_pm_prefix, "live_markets.jsonl")
        prices_key = _k(raw_pm_prefix, "live_prices.jsonl")
        write_jsonl_records(markets, markets_key)
        write_jsonl_records(prices, prices_key)
        written["raw_polymarket_markets"] = markets_key
        written["raw_polymarket_prices"] = prices_key

    if collect_binance:
        klines, _bn_meta = collect_binance_raw(
            limit=binance_limit,
            start_time=start_time,
            end_time=end_time,
        )
        klines_key = _k(raw_bn_prefix, "live_klines.jsonl")
        write_jsonl_records(klines, klines_key)
        written["raw_binance_klines"] = klines_key

    if collect_gdelt:
        if gdelt_start_date is None or gdelt_end_date is None:
            start_default = (rd - timedelta(days=1)).isoformat()
            end_default = rd.isoformat()
            start_date = gdelt_start_date or start_default
            end_date = gdelt_end_date or end_default
        else:
            start_date = gdelt_start_date
            end_date = gdelt_end_date
        timeline = collect_gdelt_raw(
            start_date=start_date,
            end_date=end_date,
            narratives_config_path=narratives_config_path,
            min_interval_sec=gdelt_min_interval_sec,
            max_attempts=gdelt_max_attempts,
            timeout=gdelt_timeout,
        )
        timeline_key = _k(raw_gd_prefix, "live_timeline.jsonl")
        write_jsonl_records(timeline, timeline_key)
        written["raw_gdelt_timeline"] = timeline_key

    return written
