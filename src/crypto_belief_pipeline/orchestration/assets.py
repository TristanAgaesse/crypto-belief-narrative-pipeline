from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml
from dagster import (
    AssetDep,
    AssetKey,
    AssetOut,
    DailyPartitionsDefinition,
    MetadataValue,
    Output,
    asset,
    multi_asset,
)

from crypto_belief_pipeline.collectors.binance import collect_binance_raw
from crypto_belief_pipeline.collectors.gdelt import collect_gdelt_raw
from crypto_belief_pipeline.collectors.polymarket import collect_polymarket_raw
from crypto_belief_pipeline.dq.soda import run_soda_checks
from crypto_belief_pipeline.features.build_gold import build_gold_tables
from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.read import read_jsonl_records, read_parquet_df
from crypto_belief_pipeline.lake.write import write_jsonl_records, write_parquet_df
from crypto_belief_pipeline.orchestration.resources import resolve_run_date
from crypto_belief_pipeline.quality.issues import detect_data_issues, write_data_issues_reports
from crypto_belief_pipeline.sample_data import load_sample_jsonl
from crypto_belief_pipeline.transform.normalize_binance import (
    normalize_klines,
    to_crypto_candles_1m,
)
from crypto_belief_pipeline.transform.normalize_gdelt import normalize_timeline, to_narrative_counts
from crypto_belief_pipeline.transform.normalize_polymarket import (
    normalize_markets,
    normalize_price_snapshots,
    to_belief_price_snapshots,
)

partitions_def = DailyPartitionsDefinition(start_date="2026-05-06")


def _k(prefix: str, name: str) -> str:
    return f"{prefix}/{name}"


def _read_yaml_mapping(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data if isinstance(data, dict) else {}


@asset(
    partitions_def=partitions_def,
    description="Write deterministic sample raw JSONL inputs into the lake (raw/).",
)
def raw_sample_inputs(context) -> dict[str, str]:
    """Write sample JSONL into the lake under raw/ (partitioned by run_date)."""

    run_date = resolve_run_date(context.partition_key)
    pm_markets = load_sample_jsonl("polymarket_markets_sample.jsonl")
    pm_prices = load_sample_jsonl("polymarket_prices_sample.jsonl")
    bn_klines = load_sample_jsonl("binance_klines_sample.jsonl")
    gd_timeline = load_sample_jsonl("gdelt_timeline_sample.jsonl")

    raw_pm = partition_path("raw", "provider=polymarket", run_date)
    raw_bn = partition_path("raw", "provider=binance", run_date)
    raw_gd = partition_path("raw", "provider=gdelt", run_date)

    written = {
        "raw_polymarket_markets": _k(raw_pm, "sample_markets.jsonl"),
        "raw_polymarket_prices": _k(raw_pm, "sample_prices.jsonl"),
        "raw_binance_klines": _k(raw_bn, "sample_klines.jsonl"),
        "raw_gdelt_timeline": _k(raw_gd, "sample_timeline.jsonl"),
    }
    write_jsonl_records(pm_markets, written["raw_polymarket_markets"])
    write_jsonl_records(pm_prices, written["raw_polymarket_prices"])
    write_jsonl_records(bn_klines, written["raw_binance_klines"])
    write_jsonl_records(gd_timeline, written["raw_gdelt_timeline"])

    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    description=(
        "Collect Polymarket live markets + prices into raw JSONL (raw/provider=polymarket)."
    ),
)
def raw_polymarket(context) -> dict[str, str]:
    """Collect Polymarket markets + prices and write raw JSONL to the lake."""

    run_date = resolve_run_date(context.partition_key)
    cfg = _read_yaml_mapping("config/markets_keywords.yaml").get("polymarket") or {}
    keywords = cfg.get("keywords") or []
    active = bool(cfg.get("active", True))
    closed = bool(cfg.get("closed", False))

    markets, prices = collect_polymarket_raw(
        limit=int(cfg.get("limit", 100)),
        keywords=list(keywords) if isinstance(keywords, list) else [],
        active=active,
        closed=closed,
    )

    raw_pm = partition_path("raw", "provider=polymarket", run_date)
    markets_key = _k(raw_pm, "live_markets.jsonl")
    prices_key = _k(raw_pm, "live_prices.jsonl")
    write_jsonl_records(markets, markets_key)
    write_jsonl_records(prices, prices_key)

    written = {"raw_polymarket_markets": markets_key, "raw_polymarket_prices": prices_key}
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "markets_rows": len(markets),
            "prices_rows": len(prices),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    description="Collect Binance live klines into raw JSONL (raw/provider=binance).",
)
def raw_binance(context) -> dict[str, str]:
    """Collect recent Binance klines and write raw JSONL to the lake."""

    run_date = resolve_run_date(context.partition_key)
    klines = collect_binance_raw(limit=120)
    raw_bn = partition_path("raw", "provider=binance", run_date)
    key = _k(raw_bn, "live_klines.jsonl")
    write_jsonl_records(klines, key)

    written = {"raw_binance_klines": key}
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": len(klines),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    description=(
        "Collect GDELT TimelineVol into raw JSONL (raw/provider=gdelt). "
        "Optional/unreliable; may be empty."
    ),
)
def raw_gdelt(context) -> dict[str, str]:
    """Collect GDELT TimelineVol (optional; may produce zero rows without failing)."""

    run_date = resolve_run_date(context.partition_key)
    # Default to the previous full UTC day, matching existing live collector behavior.
    start_date = (run_date - timedelta(days=1)).isoformat()
    end_date = run_date.isoformat()

    timeline = collect_gdelt_raw(start_date=start_date, end_date=end_date)
    raw_gd = partition_path("raw", "provider=gdelt", run_date)
    key = _k(raw_gd, "live_timeline.jsonl")
    write_jsonl_records(timeline, key)

    written = {"raw_gdelt_timeline": key}
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": len(timeline),
            "window": f"{start_date}..{end_date}",
        }
    )
    return written


def _safe_read_jsonl(key: str) -> list[dict]:
    try:
        return read_jsonl_records(key)
    except Exception:
        return []


@asset(
    partitions_def=partitions_def,
    description="Normalize Polymarket raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_polymarket(context) -> dict[str, str]:
    run_date = resolve_run_date(context.partition_key)
    raw_pm = partition_path("raw", "provider=polymarket", run_date)
    candidates = {
        "raw_polymarket_markets": [
            _k(raw_pm, "live_markets.jsonl"),
            _k(raw_pm, "sample_markets.jsonl"),
        ],
        "raw_polymarket_prices": [
            _k(raw_pm, "live_prices.jsonl"),
            _k(raw_pm, "sample_prices.jsonl"),
        ],
    }
    markets_key = next(
        (k for k in candidates["raw_polymarket_markets"] if _safe_read_jsonl(k) != []), None
    )
    prices_key = next(
        (k for k in candidates["raw_polymarket_prices"] if _safe_read_jsonl(k) != []), None
    )
    markets = _safe_read_jsonl(markets_key or candidates["raw_polymarket_markets"][0])
    prices = _safe_read_jsonl(prices_key or candidates["raw_polymarket_prices"][0])

    bronze_pm = partition_path("bronze", "provider=polymarket", run_date)
    markets_df = normalize_markets(markets)
    prices_df = normalize_price_snapshots(prices)

    markets_key = _k(bronze_pm, "markets.parquet")
    prices_key = _k(bronze_pm, "prices.parquet")
    write_parquet_df(markets_df, markets_key)
    write_parquet_df(prices_df, prices_key)

    written = {"bronze_polymarket_markets": markets_key, "bronze_polymarket_prices": prices_key}
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "markets_rows": int(getattr(markets_df, "height", 0)),
            "prices_rows": int(getattr(prices_df, "height", 0)),
            "raw_used": MetadataValue.json(
                {
                    "raw_polymarket_markets": (
                        markets_key or candidates["raw_polymarket_markets"][0]
                    ),
                    "raw_polymarket_prices": prices_key or candidates["raw_polymarket_prices"][0],
                }
            ),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    description="Normalize Binance raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_binance(context) -> dict[str, str]:
    run_date = resolve_run_date(context.partition_key)
    raw_bn = partition_path("raw", "provider=binance", run_date)
    candidates = [_k(raw_bn, "live_klines.jsonl"), _k(raw_bn, "sample_klines.jsonl")]
    chosen = next((k for k in candidates if _safe_read_jsonl(k) != []), None) or candidates[0]
    klines = _safe_read_jsonl(chosen)
    bronze_df = normalize_klines(klines)

    bronze_bn = partition_path("bronze", "provider=binance", run_date)
    key = _k(bronze_bn, "klines.parquet")
    write_parquet_df(bronze_df, key)

    written = {"bronze_binance_klines": key}
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(bronze_df, "height", 0)),
            "raw_used": chosen,
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    description="Normalize GDELT raw JSONL into bronze Parquet (typed, source-shaped).",
)
def bronze_gdelt(context) -> dict[str, str]:
    run_date = resolve_run_date(context.partition_key)
    raw_gd = partition_path("raw", "provider=gdelt", run_date)
    candidates = [_k(raw_gd, "live_timeline.jsonl"), _k(raw_gd, "sample_timeline.jsonl")]
    chosen = next((k for k in candidates if _safe_read_jsonl(k) != []), None) or candidates[0]
    timeline = _safe_read_jsonl(chosen)
    bronze_df = normalize_timeline(timeline)

    bronze_gd = partition_path("bronze", "provider=gdelt", run_date)
    key = _k(bronze_gd, "timeline.parquet")
    write_parquet_df(bronze_df, key)

    written = {"bronze_gdelt_timeline": key}
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(bronze_df, "height", 0)),
            "raw_used": chosen,
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    deps=[bronze_polymarket],
    description="Build silver belief_price_snapshots from bronze Polymarket prices.",
)
def silver_belief_price_snapshots(context, bronze_polymarket: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date(context.partition_key)
    prices_df = read_parquet_df(bronze_polymarket["bronze_polymarket_prices"])
    belief_df = to_belief_price_snapshots(prices_df)

    silver_prefix = partition_path("silver", "belief_price_snapshots", run_date)
    key = _k(silver_prefix, "data.parquet")
    write_parquet_df(belief_df, key)

    written = {"silver_belief_price_snapshots": key}
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(belief_df, "height", 0)),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    deps=[bronze_binance],
    description="Build silver crypto_candles_1m from bronze Binance klines.",
)
def silver_crypto_candles_1m(context, bronze_binance: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date(context.partition_key)
    klines_df = read_parquet_df(bronze_binance["bronze_binance_klines"])
    candles_df = to_crypto_candles_1m(klines_df)

    silver_prefix = partition_path("silver", "crypto_candles_1m", run_date)
    key = _k(silver_prefix, "data.parquet")
    write_parquet_df(candles_df, key)

    written = {"silver_crypto_candles_1m": key}
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(candles_df, "height", 0)),
        }
    )
    return written


@asset(
    partitions_def=partitions_def,
    deps=[bronze_gdelt],
    description="Build silver narrative_counts from bronze GDELT timeline series (may be empty).",
)
def silver_narrative_counts(context, bronze_gdelt: dict[str, str]) -> dict[str, str]:
    run_date = resolve_run_date(context.partition_key)
    timeline_df = read_parquet_df(bronze_gdelt["bronze_gdelt_timeline"])
    counts_df = to_narrative_counts(timeline_df)

    silver_prefix = partition_path("silver", "narrative_counts", run_date)
    key = _k(silver_prefix, "data.parquet")
    write_parquet_df(counts_df, key)

    written = {"silver_narrative_counts": key}
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "written_keys": list(written.keys()),
            "rows": int(getattr(counts_df, "height", 0)),
        }
    )
    return written


@multi_asset(
    partitions_def=partitions_def,
    deps=[silver_belief_price_snapshots, silver_crypto_candles_1m, silver_narrative_counts],
    outs={"gold_training_examples": AssetOut(), "gold_alpha_events": AssetOut()},
    description="Build gold tables (training_examples + alpha_events) from silver inputs.",
)
def gold_tables(context) -> tuple[Output[dict[str, str]], Output[dict[str, str]]]:
    """Build gold tables once and expose both outputs as distinct assets."""

    run_date = resolve_run_date(context.partition_key)
    written = build_gold_tables(run_date=run_date.isoformat())

    # build_gold_tables returns keys: training_examples, alpha_events
    training_key = written["training_examples"]
    alpha_key = written["alpha_events"]

    md = {"run_date": run_date.isoformat(), "written_keys": ["training_examples", "alpha_events"]}
    out_training = Output(
        value={"gold_training_examples": training_key},
        metadata={**md, "s3_key": training_key},
    )
    out_alpha = Output(
        value={"gold_alpha_events": alpha_key},
        metadata={**md, "s3_key": alpha_key},
    )
    return out_training, out_alpha


@asset(
    partitions_def=partitions_def,
    deps=[
        AssetDep(AssetKey("gold_training_examples")),
        AssetDep(AssetKey("gold_alpha_events")),
    ],
    description="Run Soda Core checks against DuckDB external Parquet views for this partition.",
)
def soda_data_quality(context) -> dict[str, Any]:
    run_date = resolve_run_date(context.partition_key)
    summary = run_soda_checks(run_date=run_date.isoformat())
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "passed": bool(summary.get("passed")),
            "failed_checks": MetadataValue.json(summary.get("failed_checks") or []),
            "report_paths": MetadataValue.json(summary.get("report_paths") or {}),
        }
    )
    return summary


@asset(
    partitions_def=partitions_def,
    deps=[
        soda_data_quality,
        AssetDep(AssetKey("gold_training_examples")),
        AssetDep(AssetKey("gold_alpha_events")),
    ],
    description="Detect domain-specific pipeline/data issues and write markdown/json reports.",
)
def data_issues(context) -> list[dict]:
    run_date = resolve_run_date(context.partition_key)
    issues = detect_data_issues(run_date=run_date.isoformat())
    paths = write_data_issues_reports(issues)
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "issues_count": len(issues),
            "report_paths": MetadataValue.json(paths),
        }
    )
    return issues


@asset(
    partitions_def=partitions_def,
    deps=[data_issues, soda_data_quality],
    description="Write a lightweight markdown index that links to Soda + data-issues reports.",
)
def markdown_reports(
    context, data_issues: list[dict], soda_data_quality: dict[str, Any]
) -> dict[str, str]:
    """Produce a small index markdown that links to quality + issues outputs."""

    run_date = resolve_run_date(context.partition_key)
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    index_path = reports_dir / "index.md"

    soda_paths = soda_data_quality.get("report_paths") or {}
    issues_json = "reports/data_issues.json"
    issues_md = "reports/data_issues.md"

    lines = [
        f"# Reports ({run_date.isoformat()})",
        "",
        "## Data quality (Soda)",
        f"- Output: `{soda_paths.get('output_txt', 'reports/soda_scan_output.txt')}`",
        f"- Summary: `{soda_paths.get('summary_json', 'reports/soda_scan_summary.json')}`",
        "",
        "## Data issues (domain detector)",
        f"- Markdown: `{issues_md}`",
        f"- JSON: `{issues_json}`",
        "",
        "## Quick stats",
        f"- Issues: {len(data_issues)}",
        f"- Soda passed: {bool(soda_data_quality.get('passed'))}",
        "",
    ]
    index_path.write_text("\n".join(lines), encoding="utf-8")

    out = {"index_md": str(index_path)}
    context.add_output_metadata(
        {"run_date": run_date.isoformat(), "report_paths": MetadataValue.json(out)}
    )
    return out


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
