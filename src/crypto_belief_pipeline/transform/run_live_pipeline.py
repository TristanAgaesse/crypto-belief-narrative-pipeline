from __future__ import annotations

from collections.abc import Iterable
from datetime import date

from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.read import read_jsonl_records
from crypto_belief_pipeline.transform.pipeline_steps import (
    normalize_binance_to_silver,
    normalize_gdelt_to_silver,
    normalize_polymarket_to_silver,
)

_KNOWN_SOURCES = frozenset({"polymarket", "binance", "gdelt"})


def _stable_items(d: dict[str, str]) -> Iterable[tuple[str, str]]:
    for k in sorted(d.keys()):
        yield k, d[k]


def _k(prefix: str, name: str) -> str:
    return f"{prefix}/{name}"


def _validate_sources(sources: Iterable[str] | None) -> set[str]:
    if sources is None:
        return set(_KNOWN_SOURCES)
    out = {s.strip().lower() for s in sources if s and s.strip()}
    unknown = sorted(out - _KNOWN_SOURCES)
    if unknown:
        raise ValueError(f"Unknown sources: {unknown}. Expected subset of {sorted(_KNOWN_SOURCES)}")
    return out


def run_live_pipeline(
    run_date: date | str,
    *,
    sources: Iterable[str] | None = None,
    raw_polymarket_markets_key: str | None = None,
    raw_polymarket_prices_key: str | None = None,
    raw_binance_klines_key: str | None = None,
    raw_gdelt_timeline_key: str | None = None,
) -> dict[str, str]:
    """Normalize live raw JSONL into bronze+silver Parquet for one run_date.

    This is the CLI/operator path for the Step 3 "daily keys" live collectors
    in `collectors/run_live_collectors.py`:
      - raw/provider=polymarket/date=.../live_markets.jsonl
      - raw/provider=polymarket/date=.../live_prices.jsonl
      - raw/provider=binance/date=.../live_klines.jsonl
      - raw/provider=gdelt/date=.../live_timeline.jsonl

    Selected ``sources`` (default: all known) determines which sources are
    processed. Unselected sources are explicitly skipped — there is no fallback
    to default raw keys, so partial-source runs can never silently mix new and
    stale raw inputs.

    Dagster micro-batch assets use separate ``hour=.../batch_id=...`` keys and
    are intentionally handled in ``orchestration/assets.py`` instead.
    """

    selected = _validate_sources(sources)

    raw_pm_prefix = partition_path("raw", "provider=polymarket", run_date)
    raw_bn_prefix = partition_path("raw", "provider=binance", run_date)
    raw_gd_prefix = partition_path("raw", "provider=gdelt", run_date)

    raw_polymarket_markets_key = raw_polymarket_markets_key or _k(
        raw_pm_prefix, "live_markets.jsonl"
    )
    raw_polymarket_prices_key = raw_polymarket_prices_key or _k(raw_pm_prefix, "live_prices.jsonl")
    raw_binance_klines_key = raw_binance_klines_key or _k(raw_bn_prefix, "live_klines.jsonl")
    raw_gdelt_timeline_key = raw_gdelt_timeline_key or _k(raw_gd_prefix, "live_timeline.jsonl")

    written: dict[str, str] = {}

    if "polymarket" in selected:
        pm_markets = read_jsonl_records(raw_polymarket_markets_key)
        pm_prices = read_jsonl_records(raw_polymarket_prices_key)
        written.update(
            normalize_polymarket_to_silver(
                run_date=run_date,
                markets_records=pm_markets,
                prices_records=pm_prices,
            )
        )

    if "binance" in selected:
        bn_klines = read_jsonl_records(raw_binance_klines_key)
        written.update(
            normalize_binance_to_silver(
                run_date=run_date,
                klines_records=bn_klines,
            )
        )

    if "gdelt" in selected:
        gd_timeline = read_jsonl_records(raw_gdelt_timeline_key)
        written.update(
            normalize_gdelt_to_silver(
                run_date=run_date,
                timeline_records=gd_timeline,
            )
        )

    written["__sources_processed__"] = ",".join(sorted(selected))
    written["__sources_skipped__"] = ",".join(sorted(_KNOWN_SOURCES - selected))

    return dict(_stable_items(written))


__all__ = ["run_live_pipeline"]
