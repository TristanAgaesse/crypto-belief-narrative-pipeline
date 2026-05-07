"""Gold + quality + reporting Dagster assets.

Includes:
- ``gold_tables``: build training_examples + live_signals from silver inputs.
- ``soda_data_quality``: Soda Core checks over DuckDB external views.
- ``data_issues``: domain-specific issue detection (writes ``reports/data_issues.*``).
- ``markdown_reports``: lightweight index linking to Soda and data-issues outputs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from crypto_belief_pipeline.dq.soda import run_soda_checks
from crypto_belief_pipeline.features.build_gold import build_gold_tables
from crypto_belief_pipeline.orchestration._helpers import partitions_def
from crypto_belief_pipeline.orchestration.assets_transform import (
    silver_belief_price_snapshots,
    silver_crypto_candles_1m,
    silver_narrative_counts,
)
from crypto_belief_pipeline.orchestration.resources import resolve_run_date_from_context
from crypto_belief_pipeline.quality.issues import detect_data_issues, write_data_issues_reports
from crypto_belief_pipeline.reports.index_md import render_reports_index_md
from dagster import (
    AssetDep,
    AssetKey,
    AssetOut,
    MetadataValue,
    Output,
    asset,
    multi_asset,
)


@multi_asset(
    partitions_def=partitions_def,
    deps=[silver_belief_price_snapshots, silver_crypto_candles_1m, silver_narrative_counts],
    outs={"gold_training_examples": AssetOut(), "gold_live_signals": AssetOut()},
    description="Build gold tables (training_examples + live_signals) from silver inputs.",
)
def gold_tables(
    context,
    silver_belief_price_snapshots: dict[str, str],
    silver_crypto_candles_1m: dict[str, str],
    silver_narrative_counts: dict[str, str],
) -> tuple[Output[dict[str, str]], Output[dict[str, str]]]:
    """Build gold tables once and expose both outputs as distinct assets."""

    run_date = resolve_run_date_from_context(context)
    written = build_gold_tables(
        run_date=run_date.isoformat(),
        belief_key=silver_belief_price_snapshots["silver_belief_price_snapshots"],
        candles_key=silver_crypto_candles_1m["silver_crypto_candles_1m"],
        narrative_key=silver_narrative_counts["silver_narrative_counts"],
    )

    training_key = written["training_examples"]
    live_key = written["live_signals"]

    written_keys_md = MetadataValue.json(["training_examples", "live_signals"])
    out_training = Output(
        value={"gold_training_examples": training_key},
        metadata={
            "run_date": run_date.isoformat(),
            "written_keys": written_keys_md,
            "s3_key": training_key,
        },
    )
    out_live = Output(
        value={"gold_live_signals": live_key},
        metadata={
            "run_date": run_date.isoformat(),
            "written_keys": written_keys_md,
            "s3_key": live_key,
        },
    )
    return out_training, out_live


@asset(
    partitions_def=partitions_def,
    deps=[
        AssetDep(AssetKey("gold_training_examples")),
        AssetDep(AssetKey("gold_live_signals")),
    ],
    description="Run Soda Core checks against DuckDB external Parquet views for this partition.",
)
def soda_data_quality(context) -> dict[str, Any]:
    run_date = resolve_run_date_from_context(context)
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
        AssetDep(AssetKey("gold_live_signals")),
    ],
    description="Detect domain-specific pipeline/data issues and write markdown/json reports.",
)
def data_issues(context) -> list[dict]:
    run_date = resolve_run_date_from_context(context)
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

    run_date = resolve_run_date_from_context(context)
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    index_path = reports_dir / "index.md"

    soda_paths = soda_data_quality.get("report_paths") or {}
    body = render_reports_index_md(
        run_date.isoformat(),
        issues_count=len(data_issues),
        soda_passed=bool(soda_data_quality.get("passed")),
        soda_paths=soda_paths if isinstance(soda_paths, dict) else None,
    )
    index_path.write_text(body, encoding="utf-8")

    out = {"index_md": str(index_path)}
    context.add_output_metadata(
        {"run_date": run_date.isoformat(), "report_paths": MetadataValue.json(out)}
    )
    return out


__all__ = ["data_issues", "gold_tables", "markdown_reports", "soda_data_quality"]
