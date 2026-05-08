"""Gold + quality + reporting Dagster assets.

Includes:
- ``gold_tables``: build training_examples + live_signals from silver inputs.
- ``soda_data_quality``: Soda Core checks over DuckDB external views.
- ``data_issues``: domain-specific issue detection (writes ``reports/data_issues.*``).
- ``markdown_reports``: lightweight index linking to Soda and data-issues outputs.
"""

from __future__ import annotations

from typing import Any

from crypto_belief_pipeline.dq.soda import run_soda_checks
from crypto_belief_pipeline.features.build_gold import build_gold_tables
from crypto_belief_pipeline.orchestration._helpers import hourly_partitions_def
from crypto_belief_pipeline.orchestration.assets_transform import (
    silver_belief_price_snapshots,
    silver_crypto_candles_1m,
    silver_narrative_counts,
)
from crypto_belief_pipeline.orchestration.resources import resolve_run_date_from_context
from crypto_belief_pipeline.quality.issues import detect_data_issues, write_data_issues_reports
from crypto_belief_pipeline.reports.index_md import (
    render_reports_index_md,
    reports_dir_for_partition,
)
from crypto_belief_pipeline.state.processing_watermarks import read_processing_watermark
from dagster import (
    AssetDep,
    AssetKey,
    AssetOut,
    MetadataValue,
    Output,
    asset,
    multi_asset,
)

# Bronze layer writes these watermarks (see ``assets_transform``); bounded reads per partition.
_BRONZE_WATERMARK_LOOKUPS: tuple[tuple[str, str], ...] = (
    ("bronze_polymarket", "polymarket"),
    ("bronze_binance", "binance"),
    ("bronze_gdelt", "gdelt"),
    ("bronze_kalshi", "kalshi"),
)


def _scan_processing_gaps_for_partition(partition_key: str) -> list[dict[str, Any]]:
    """Return non-ok bronze watermarks for this hourly partition only (O(1) S3 reads)."""
    gaps: list[dict[str, Any]] = []
    for consumer_asset, source in _BRONZE_WATERMARK_LOOKUPS:
        wm = read_processing_watermark(
            consumer_asset=consumer_asset,
            source=source,
            partition_key=partition_key,
        )
        if wm is None:
            continue
        if wm.status != "ok":
            gaps.append(
                {
                    "consumer_asset": consumer_asset,
                    "source": wm.source or source,
                    "partition_key": wm.partition_key or partition_key,
                    "missing_key": wm.last_processed_batch_id or "",
                    "reason": f"watermark_status={wm.status}",
                }
            )
    return gaps


@multi_asset(
    partitions_def=hourly_partitions_def,
    deps=[silver_belief_price_snapshots, silver_crypto_candles_1m, silver_narrative_counts],
    outs={"gold_training_examples": AssetOut(), "gold_live_signals": AssetOut()},
    description="Build gold tables (training_examples + live_signals) from silver inputs.",
)
def gold_tables(
    context,
) -> tuple[Output[dict[str, str]], Output[dict[str, str]]]:
    """Build gold tables once and expose both outputs as distinct assets."""

    run_date = resolve_run_date_from_context(context)
    written = build_gold_tables(
        run_date=run_date.isoformat(),
        partition_key=context.partition_key if context.has_partition_key else None,
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
    partitions_def=hourly_partitions_def,
    deps=[
        AssetDep(AssetKey("gold_training_examples")),
        AssetDep(AssetKey("gold_live_signals")),
    ],
    description="Run Soda Core checks against DuckDB external Parquet views for this partition.",
)
def soda_data_quality(context) -> dict[str, Any]:
    run_date = resolve_run_date_from_context(context)
    partition_key = context.partition_key if context.has_partition_key else None
    reports_dir = reports_dir_for_partition(run_date.isoformat(), partition_key)
    summary = run_soda_checks(
        run_date=run_date.isoformat(),
        partition_key=partition_key,
        reports_dir=reports_dir,
    )
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
    partitions_def=hourly_partitions_def,
    deps=[
        soda_data_quality,
        AssetDep(AssetKey("gold_training_examples")),
        AssetDep(AssetKey("gold_live_signals")),
    ],
    description="Detect domain-specific pipeline/data issues and write markdown/json reports.",
)
def data_issues(context) -> list[dict]:
    run_date = resolve_run_date_from_context(context)
    partition_key = context.partition_key if context.has_partition_key else None
    reports_dir = reports_dir_for_partition(run_date.isoformat(), partition_key)
    issues = detect_data_issues(run_date=run_date.isoformat(), partition_key=partition_key)
    paths = write_data_issues_reports(
        issues,
        md_path=reports_dir / "data_issues.md",
        json_path=reports_dir / "data_issues.json",
    )
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "issues_count": len(issues),
            "report_paths": MetadataValue.json(paths),
        }
    )
    return issues


@asset(
    partitions_def=hourly_partitions_def,
    deps=[data_issues, soda_data_quality],
    description="Write a lightweight markdown index that links to Soda + data-issues reports.",
)
def markdown_reports(
    context, data_issues: list[dict], soda_data_quality: dict[str, Any]
) -> dict[str, str]:
    """Produce a small index markdown that links to quality + issues outputs."""

    run_date = resolve_run_date_from_context(context)
    partition_key = context.partition_key if context.has_partition_key else None
    reports_dir = reports_dir_for_partition(run_date.isoformat(), partition_key)
    reports_dir.mkdir(parents=True, exist_ok=True)
    index_path = reports_dir / "index.md"

    soda_paths = soda_data_quality.get("report_paths") or {}
    body = render_reports_index_md(
        run_date.isoformat(),
        issues_count=len(data_issues),
        soda_passed=bool(soda_data_quality.get("passed")),
        soda_paths=soda_paths if isinstance(soda_paths, dict) else None,
        issues_paths={
            "md": str(reports_dir / "data_issues.md"),
            "json": str(reports_dir / "data_issues.json"),
        },
    )
    index_path.write_text(body, encoding="utf-8")

    out = {"index_md": str(index_path)}
    context.add_output_metadata(
        {"run_date": run_date.isoformat(), "report_paths": MetadataValue.json(out)}
    )
    return out


@asset(
    partitions_def=hourly_partitions_def,
    deps=[soda_data_quality],
    description=(
        "For this hourly partition, read bronze processing watermarks only (bounded S3 reads) "
        "and surface non-ok statuses as structured gap records."
    ),
)
def processing_gaps(context) -> list[dict[str, Any]]:
    run_date = resolve_run_date_from_context(context)
    partition_key = context.partition_key if context.has_partition_key else ""
    gaps = _scan_processing_gaps_for_partition(partition_key) if partition_key else []
    context.add_output_metadata(
        {
            "run_date": run_date.isoformat(),
            "partition_key": partition_key,
            "gaps_count": len(gaps),
            "gaps_sample": MetadataValue.json(gaps[:20]),
        }
    )
    return gaps


__all__ = ["data_issues", "gold_tables", "markdown_reports", "processing_gaps", "soda_data_quality"]
