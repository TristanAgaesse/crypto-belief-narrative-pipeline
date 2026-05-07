from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from crypto_belief_pipeline.features.market_tags import load_market_tags
from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.read import read_parquet_df


def _as_date_str(run_date: date | str) -> str:
    return run_date.isoformat() if isinstance(run_date, date) else str(run_date)


def _safe_read_parquet(key: str) -> pl.DataFrame:
    try:
        return read_parquet_df(key)
    except Exception:
        return pl.DataFrame()


def _issue(
    *,
    severity: str,
    category: str,
    source: str,
    issue: str,
    message: str,
    suggested_action: str,
) -> dict[str, str]:
    return {
        "severity": severity,
        "category": category,
        "source": source,
        "issue": issue,
        "message": message,
        "suggested_action": suggested_action,
    }


def detect_data_issues(
    run_date: date | str,
    market_tags_path: str | Path = "data/sample/market_tags.csv",
) -> list[dict]:
    rd = _as_date_str(run_date)

    # Silver keys
    belief_key = f"{partition_path('silver', 'belief_price_snapshots', rd)}/data.parquet"
    candles_key = f"{partition_path('silver', 'crypto_candles_1m', rd)}/data.parquet"
    narrative_key = f"{partition_path('silver', 'narrative_counts', rd)}/data.parquet"

    # Gold keys
    training_key = f"{partition_path('gold', 'training_examples', rd)}/data.parquet"
    live_key = f"{partition_path('gold', 'live_signals', rd)}/data.parquet"

    belief = _safe_read_parquet(belief_key)
    candles = _safe_read_parquet(candles_key)
    narrative = _safe_read_parquet(narrative_key)
    training = _safe_read_parquet(training_key)
    live = _safe_read_parquet(live_key)

    issues: list[dict] = []

    # Critical empties
    if candles.height == 0:
        issues.append(
            _issue(
                severity="critical",
                category="source_availability",
                source="binance",
                issue="crypto_candles_empty",
                message="No rows in silver_crypto_candles_1m for this run_date.",
                suggested_action=(
                    "Verify Binance collector connectivity and backfill more candle history."
                ),
            )
        )

    if belief.height == 0:
        issues.append(
            _issue(
                severity="critical",
                category="source_availability",
                source="polymarket",
                issue="belief_price_snapshots_empty",
                message="No rows in silver_belief_price_snapshots for this run_date.",
                suggested_action=(
                    "Verify Polymarket collector coverage/filters and confirm markets are being "
                    "discovered."
                ),
            )
        )

    # GDELT optional but surfaced
    if narrative.height == 0:
        issues.append(
            _issue(
                severity="high",
                category="source_availability",
                source="gdelt",
                issue="narrative_counts_empty",
                message=(
                    "No rows in silver_narrative_counts for this run_date "
                    "(GDELT may be rate-limited)."
                ),
                suggested_action=(
                    "Disable GDELT on fast schedules or backfill with slower windows / "
                    "add a fallback provider."
                ),
            )
        )

    # Missing future labels rate
    if training.height == 0:
        issues.append(
            _issue(
                severity="warning",
                category="research_validity",
                source="gold",
                issue="gold_training_examples_empty",
                message="gold_training_examples has 0 rows for this run_date.",
                suggested_action=(
                    "Ensure market tags are present and silver inputs are non-empty for "
                    "the run_date."
                ),
            )
        )
    else:
        if "future_ret_4h" in training.columns:
            missing_rate = float(training.select(pl.col("future_ret_4h").is_null().mean()).item())
            if missing_rate > 0.8:
                issues.append(
                    _issue(
                        severity="high",
                        category="label_availability",
                        source="binance",
                        issue="missing_future_ret_4h_high",
                        message=(
                            f"future_ret_4h is missing for {missing_rate:.0%} of "
                            "gold_training_examples rows."
                        ),
                        suggested_action=(
                            "Backfill more Binance history or switch to as-of joins with tolerance "
                            "for event_time."
                        ),
                    )
                )

    # Market tag mapping coverage (uses silver belief universe)
    if belief.height > 0 and "market_id" in belief.columns:
        tags = load_market_tags(str(market_tags_path))
        tag_ids = set(tags["market_id"].to_list()) if "market_id" in tags.columns else set()
        belief_ids = set(belief["market_id"].to_list())
        total = len(belief_ids)
        unmapped = len([m for m in belief_ids if m not in tag_ids])
        unmapped_rate = (unmapped / total) if total else 0.0
        if unmapped_rate > 0.5:
            issues.append(
                _issue(
                    severity="warning",
                    category="coverage",
                    source="polymarket",
                    issue="unmapped_market_rate_high",
                    message=(
                        f"{unmapped_rate:.0%} of markets in silver_belief_price_snapshots are not "
                        "present in market_tags."
                    ),
                    suggested_action=(
                        "Add/curate market tags for the most relevant/high-volume markets."
                    ),
                )
            )

    # Wide spread rate (threshold aligned to scoring spread penalty scale ~0.10)
    if training.height > 0 and "spread" in training.columns:
        wide_rate = float(training.select((pl.col("spread").fill_null(0.0) >= 0.10).mean()).item())
        if wide_rate > 0.3:
            issues.append(
                _issue(
                    severity="warning",
                    category="market_quality",
                    source="polymarket",
                    issue="wide_spread_rate_high",
                    message=f"{wide_rate:.0%} of gold_training_examples rows have spread >= 0.10.",
                    suggested_action=(
                        "Filter illiquid markets or adjust candidate thresholds; "
                        "consider using mid-price or deeper liquidity signals."
                    ),
                )
            )

    # Duplicate candles
    if candles.height > 0 and {"asset", "timestamp"}.issubset(set(candles.columns)):
        dup_count = candles.group_by(["asset", "timestamp"]).len().filter(pl.col("len") > 1).height
        if dup_count > 0:
            issues.append(
                _issue(
                    severity="warning",
                    category="duplicates",
                    source="binance",
                    issue="duplicate_asset_timestamp_candles",
                    message=(
                        f"Found {dup_count} duplicated (asset, timestamp) groups in "
                        "silver_crypto_candles_1m."
                    ),
                    suggested_action=(
                        "Deduplicate candles by (asset, timestamp) during normalization or enforce "
                        "uniqueness in collectors."
                    ),
                )
            )

    # Live signals empty is informational
    if live.height == 0:
        issues.append(
            _issue(
                severity="info",
                category="research_validity",
                source="gold",
                issue="live_signals_empty",
                message="gold_live_signals has 0 rows for this run_date (may be a valid outcome).",
                suggested_action=(
                    "If unexpected, loosen candidate thresholds or validate upstream "
                    "belief/narrative signals."
                ),
            )
        )

    return issues


def write_data_issues_reports(
    issues: list[dict],
    md_path: str | Path = "reports/data_issues.md",
    json_path: str | Path = "reports/data_issues.json",
) -> dict[str, str]:
    mdp = Path(md_path)
    jsp = Path(json_path)
    mdp.parent.mkdir(parents=True, exist_ok=True)
    jsp.parent.mkdir(parents=True, exist_ok=True)

    jsp.write_text(json.dumps(issues, indent=2, sort_keys=True), encoding="utf-8")

    by_severity: dict[str, list[dict[str, Any]]] = {
        "critical": [],
        "high": [],
        "warning": [],
        "info": [],
    }
    for it in issues:
        sev = str(it.get("severity", "info"))
        by_severity.setdefault(sev, []).append(it)

    lines: list[str] = ["# Data issues", ""]
    for sev in ("critical", "high", "warning", "info"):
        items = by_severity.get(sev) or []
        lines.append(f"## {sev}")
        if not items:
            lines.append("- (none)")
            lines.append("")
            continue
        for it in items:
            lines.append(f"- **{it.get('category')}/{it.get('source')}**: {it.get('message')}")
            lines.append(f"  - issue: `{it.get('issue')}`")
            lines.append(f"  - suggested_action: {it.get('suggested_action')}")
        lines.append("")

    mdp.write_text("\n".join(lines), encoding="utf-8")
    return {"md": str(mdp), "json": str(jsp)}


__all__ = ["detect_data_issues", "write_data_issues_reports"]
