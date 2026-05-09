from __future__ import annotations

import json
import subprocess
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import typer

from crypto_belief_pipeline.collectors.run_live_collectors import run_live_collectors
from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.dq.soda import run_soda_checks
from crypto_belief_pipeline.features.build_gold import build_gold_tables
from crypto_belief_pipeline.features.market_tags import DEFAULT_MARKET_TAGS_PATH
from crypto_belief_pipeline.io_guardrails import SampleConfigError, resolve_sample_bucket
from crypto_belief_pipeline.lake.compaction import (
    compact_daily_from_hourly,
    compact_hourly_microbatches,
)
from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.read import LakeKeyNotFound, read_parquet_row_count
from crypto_belief_pipeline.lake.s3 import ensure_bucket_exists
from crypto_belief_pipeline.quality.issues import detect_data_issues, write_data_issues_reports
from crypto_belief_pipeline.reports.index_md import (
    load_issues_count_from_disk,
    load_soda_passed_from_disk,
    render_reports_index_md,
)
from crypto_belief_pipeline.reports.run_summary import (
    build_pipeline_run_summary,
    summarize_issues_by_severity,
    write_pipeline_run_summary,
)
from crypto_belief_pipeline.transform.run_live_pipeline import run_live_pipeline
from crypto_belief_pipeline.transform.run_sample_pipeline import run_sample_pipeline

app = typer.Typer(add_completion=False)
pipeline_app = typer.Typer(help="Run end-to-end pipeline flows.")
dagster_app = typer.Typer(help="Thin wrappers around Dagster entrypoints.")
dq_app = typer.Typer(help="Data quality (Soda + DuckDB).")
issues_app = typer.Typer(help="Domain-specific data issue detection.")

app.add_typer(pipeline_app, name="pipeline")
app.add_typer(dagster_app, name="dagster")
app.add_typer(dq_app, name="dq")
app.add_typer(issues_app, name="issues")

_KNOWN_SOURCES = {"polymarket", "binance", "gdelt"}


def _parse_sources(value: str | None) -> set[str]:
    if value is None:
        return set(_KNOWN_SOURCES)
    text = value.strip().lower()
    if not text or text == "all":
        return set(_KNOWN_SOURCES)
    parts = {p.strip() for p in text.split(",") if p.strip()}
    unknown = sorted(parts - _KNOWN_SOURCES)
    if unknown:
        raise typer.BadParameter(
            f"Unknown sources: {unknown}. Expected one of {sorted(_KNOWN_SOURCES)}"
        )
    return parts


def _run_date(dt: str | None) -> str:
    return dt or date.today().isoformat()


def _ensure_bucket() -> None:
    s = get_settings()
    ensure_bucket_exists(s.s3_bucket, settings=s)


def _row_counts_for_parquet_keys(
    keys: dict[str, str], *, bucket: str | None
) -> dict[str, int | None]:
    """Best-effort row counts for written Parquet objects (gold, etc.)."""

    out: dict[str, int | None] = {}
    for name, key in keys.items():
        try:
            out[name] = read_parquet_row_count(key, bucket=bucket)
        except (LakeKeyNotFound, OSError, ValueError):
            out[name] = None
    return out


def _ensure_sample_bucket() -> str:
    """Validate sample config and ensure the dedicated sample bucket exists.

    Delegates the policy check to :func:`resolve_sample_bucket` so CLI, sample
    runner, and Dagster helpers cannot drift in what they consider safe.
    """

    try:
        sample_bucket = resolve_sample_bucket()
    except SampleConfigError as e:
        raise typer.BadParameter(str(e)) from e
    ensure_bucket_exists(sample_bucket, settings=get_settings())
    return sample_bucket


@app.command("check-config")
def check_config() -> None:
    s = get_settings()
    typer.echo("OK")
    typer.echo(f"ENV={s.env}")
    typer.echo(f"AWS_ENDPOINT_URL={s.aws_endpoint_url}")
    typer.echo(f"AWS_REGION={s.aws_region}")
    typer.echo(f"S3_BUCKET={s.s3_bucket}")


@app.command("ensure-bucket")
def ensure_bucket() -> None:
    s = get_settings()
    ensure_bucket_exists(s.s3_bucket, settings=s)
    typer.echo(f"OK bucket={s.s3_bucket}")


@app.command("print-lake-prefix")
def print_lake_prefix(
    layer: str = typer.Option(..., "--layer"),
    dataset: str = typer.Option(..., "--dataset"),
    dt: str | None = typer.Option(None, "--date", help="YYYY-MM-DD"),
) -> None:
    d: str | date = dt or date.today()
    typer.echo(partition_path(layer, dataset, d))


@app.command("run-sample")
def run_sample(
    dt: str = typer.Option("2026-05-06", "--date", help="YYYY-MM-DD"),
) -> None:
    written = run_sample_pipeline(run_date=dt)
    for name in sorted(written.keys()):
        typer.echo(f"{name} {written[name]}")


# Sources required to produce a complete gold/DQ/issues run end-to-end.
# Belief silver requires polymarket; candles require binance. GDELT narrative is optional:
# gold continues with null/default narrative features when narrative silver is empty.
_GOLD_REQUIRED_SOURCES = frozenset({"polymarket", "binance"})


@pipeline_app.command("run")
def pipeline_run(
    dt: str | None = typer.Option(None, "--date", help="YYYY-MM-DD (default: today UTC)"),
    mode: str = typer.Option("live", "--mode", help="live|sample"),
    sources: str = typer.Option(
        "all",
        "--sources",
        help="Comma-separated sources for live mode: polymarket,binance,gdelt (or 'all').",
    ),
    skip_gold: bool = typer.Option(False, "--skip-gold", help="Skip silver->gold build step."),
    skip_dq: bool = typer.Option(False, "--skip-dq", help="Skip Soda data-quality checks."),
    skip_issues: bool = typer.Option(
        False, "--skip-issues", help="Skip issue detection + reports."
    ),
) -> None:
    """Run an operator-friendly end-to-end pipeline for one run date."""

    run_date = _run_date(dt)

    if mode not in {"live", "sample"}:
        raise typer.BadParameter("mode must be one of: live, sample")

    t0 = time.monotonic()
    started_at = datetime.now(tz=UTC).isoformat()
    stages: dict[str, Any] = {}
    warnings: list[str] = []

    silver_keys: dict[str, str] = {}
    target_bucket: str | None = None

    if mode == "sample":
        # Sample mode never writes to the live bucket; bucket isolation lives at
        # the bucket level (not as a key prefix). Still verify the configured
        # live bucket exists so operators catch misconfiguration early.
        _ensure_bucket()
        target_bucket = _ensure_sample_bucket()
        written = run_sample_pipeline(run_date=run_date)
        typer.echo(json.dumps(written, indent=2, sort_keys=True))
        stages["sample_raw_to_silver"] = {
            "sample_bucket": written.get("sample_bucket"),
            "keys_written": {k: v for k, v in written.items() if k != "sample_bucket"},
        }
        # Silver lives in the sample bucket with the canonical key layout, so we
        # only need to forward the keys; the bucket override is what routes I/O.
        silver_keys = {
            "belief_key": written["silver_belief_price_snapshots"],
            "candles_key": written["silver_crypto_candles_1m"],
            "narrative_key": written["silver_narrative_counts"],
        }
    else:
        src = _parse_sources(sources)

        # Partial-source live runs cannot satisfy gold's required silver inputs.
        # Without this guard, downstream stages would silently fall back to
        # default partition keys and may read stale data from a previous run.
        downstream_requested = not (skip_gold and skip_dq and skip_issues)
        if downstream_requested and not _GOLD_REQUIRED_SOURCES.issubset(src):
            missing = sorted(_GOLD_REQUIRED_SOURCES - src)
            raise typer.BadParameter(
                "Live partial-source runs cannot produce gold/DQ/issues without "
                "Polymarket and Binance silver inputs. "
                f"Missing required sources: {missing}. "
                "GDELT is optional: use e.g. --sources polymarket,binance. "
                "Either add the missing sources, re-run with --sources all (or omit --sources), "
                "or pass --skip-gold --skip-dq --skip-issues to limit the run "
                "to raw + bronze + silver."
            )

        _ensure_bucket()
        raw_written = run_live_collectors(
            run_date=run_date,
            collect_polymarket=("polymarket" in src),
            collect_binance=("binance" in src),
            collect_gdelt=("gdelt" in src),
        )
        typer.echo(json.dumps(raw_written, indent=2, sort_keys=True))
        stages["live_collectors"] = dict(raw_written)

        # Normalize selected sources only. Unselected sources are explicitly skipped so we
        # never fall back to stale default raw keys from a previous run.
        normalized = run_live_pipeline(
            run_date=run_date,
            sources=src,
            raw_polymarket_markets_key=raw_written.get("raw_polymarket_markets"),
            raw_polymarket_prices_key=raw_written.get("raw_polymarket_prices"),
            raw_binance_klines_key=raw_written.get("raw_binance_klines"),
            raw_gdelt_timeline_key=raw_written.get("raw_gdelt_timeline"),
        )
        typer.echo(json.dumps(normalized, indent=2, sort_keys=True))
        stages["live_normalize"] = {
            k: v for k, v in normalized.items() if not str(k).startswith("__")
        }
        stages["live_sources_meta"] = {
            k: v for k, v in normalized.items() if str(k).startswith("__")
        }

        # Thread silver keys explicitly into downstream stages so they never
        # reach back to default partition keys (which is what introduced the
        # stale-mix risk for partial-source runs).
        if downstream_requested:
            silver_keys = {
                "belief_key": normalized["silver_belief_price_snapshots"],
                "candles_key": normalized["silver_crypto_candles_1m"],
            }
            if nk := normalized.get("silver_narrative_counts"):
                silver_keys["narrative_key"] = nk

    if not skip_gold:
        gold_written = build_gold_tables(run_date=run_date, bucket=target_bucket, **silver_keys)
        typer.echo(json.dumps(gold_written, indent=2, sort_keys=True))
        stages["gold"] = {
            "keys": gold_written,
            "row_counts": _row_counts_for_parquet_keys(gold_written, bucket=target_bucket),
        }
    else:
        stages["gold"] = {"skipped": True}

    if not skip_dq:
        summary = run_soda_checks(run_date=run_date, bucket=target_bucket)
        typer.echo(json.dumps(summary, indent=2, sort_keys=True))
        stages["dq_soda"] = summary
        if not summary.get("passed", False):
            warnings.append("Soda data-quality checks reported failures or errors.")
    else:
        stages["dq_soda"] = {"skipped": True}

    if not skip_issues:
        issues = detect_data_issues(run_date=run_date, bucket=target_bucket)
        paths = write_data_issues_reports(issues)
        typer.echo(json.dumps({"issues": len(issues), "paths": paths}, indent=2, sort_keys=True))
        stages["issues"] = {
            "count": len(issues),
            "by_severity": summarize_issues_by_severity(issues),
            "report_paths": paths,
        }
        crit = stages["issues"]["by_severity"].get("critical", 0)
        if crit:
            warnings.append(f"Domain issue detector reported {crit} critical issue(s).")
    else:
        stages["issues"] = {"skipped": True}

    duration_s = time.monotonic() - t0
    finished_at = datetime.now(tz=UTC).isoformat()
    run_summary = build_pipeline_run_summary(
        run_date=run_date,
        mode=mode,
        duration_seconds=duration_s,
        started_at=started_at,
        finished_at=finished_at,
        stages=stages,
        warnings=warnings,
    )
    rs_paths = write_pipeline_run_summary(run_summary)
    typer.echo(json.dumps({"run_summary": rs_paths}, indent=2, sort_keys=True))


@app.command("smoke-test-apis")
def smoke_test_apis(
    strict: bool = typer.Option(False, "--strict", help="Exit non-zero if any check fails"),
) -> None:
    """Hit the live APIs with tiny queries and print a status table."""

    from crypto_belief_pipeline.collectors.binance import KLINES_URL, PING_URL
    from crypto_belief_pipeline.collectors.http import get_json
    from crypto_belief_pipeline.collectors.polymarket import GAMMA_MARKETS_URL

    rows: list[tuple[str, str, str, str]] = []
    any_failed = False

    try:
        markets = get_json(GAMMA_MARKETS_URL, params={"limit": 3})
        n = len(markets) if isinstance(markets, list) else 0
        rows.append(("polymarket", "200", "yes", f"{n} markets"))
    except Exception as e:
        any_failed = True
        rows.append(("polymarket", "err", "no", str(e)[:120]))

    try:
        get_json(PING_URL)
        klines = get_json(KLINES_URL, params={"symbol": "BTCUSDT", "interval": "1m", "limit": 1})
        n = len(klines) if isinstance(klines, list) else 0
        rows.append(("binance", "200", "yes", f"ping ok, {n} kline"))
    except Exception as e:
        any_failed = True
        rows.append(("binance", "err", "no", str(e)[:120]))

    try:
        from crypto_belief_pipeline.collectors.gdelt import fetch_timelinevol

        end = date.today()
        start = end - timedelta(days=2)
        records = fetch_timelinevol(
            narrative="bitcoin",
            query="bitcoin",
            start_date=start.isoformat(),
            end_date=end.isoformat(),
        )
        rows.append(("gdelt", "ok", "yes", f"{len(records)} rows"))
    except Exception as e:
        any_failed = True
        rows.append(("gdelt", "err", "no", str(e)[:120]))

    typer.echo(f"{'source':<12} {'status':<8} {'ok':<4} detail")
    for source, status, ok, detail in rows:
        typer.echo(f"{source:<12} {status:<8} {ok:<4} {detail}")

    if any_failed and strict:
        raise typer.Exit(code=1)


@app.command("fetch-live")
def fetch_live(
    dt: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    polymarket_limit: int = typer.Option(100, "--polymarket-limit"),
    binance_limit: int = typer.Option(120, "--binance-limit"),
    sources: str = typer.Option(
        "all",
        "--sources",
        help="Comma-separated list of sources to collect: polymarket,binance,gdelt (or 'all').",
    ),
) -> None:
    src = _parse_sources(sources)
    written = run_live_collectors(
        run_date=dt,
        collect_polymarket=("polymarket" in src),
        collect_binance=("binance" in src),
        collect_gdelt=("gdelt" in src),
        binance_limit=binance_limit,
        polymarket_limit=polymarket_limit,
    )
    for name in sorted(written.keys()):
        typer.echo(f"{name} {written[name]}")


@app.command("run-live")
def run_live(
    dt: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    polymarket_limit: int = typer.Option(100, "--polymarket-limit"),
    binance_limit: int = typer.Option(120, "--binance-limit"),
    sources: str = typer.Option(
        "all",
        "--sources",
        help="Comma-separated list of sources to collect: polymarket,binance,gdelt (or 'all').",
    ),
) -> None:
    _ensure_bucket()

    src = _parse_sources(sources)
    raw_written = run_live_collectors(
        run_date=dt,
        collect_polymarket=("polymarket" in src),
        collect_binance=("binance" in src),
        collect_gdelt=("gdelt" in src),
        binance_limit=binance_limit,
        polymarket_limit=polymarket_limit,
    )
    for name in sorted(raw_written.keys()):
        typer.echo(f"{name} {raw_written[name]}")


@app.command("build-gold")
def build_gold(
    dt: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    market_tags_path: str = typer.Option(
        DEFAULT_MARKET_TAGS_PATH,
        "--market-tags-path",
        help="Path to manually curated market tags CSV.",
    ),
) -> None:
    written = build_gold_tables(run_date=dt, market_tags_path=market_tags_path)
    for name in sorted(written.keys()):
        typer.echo(f"{name} {written[name]}")


@app.command("build-latest-gold")
def build_latest_gold(
    lookback_hours: int = typer.Option(48, "--lookback-hours", min=1),
    market_tags_path: str = typer.Option(
        DEFAULT_MARKET_TAGS_PATH,
        "--market-tags-path",
        help="Path to manually curated market tags CSV.",
    ),
) -> None:
    """Build gold for the most recent time window.

    Current implementation (MVP): build gold once per affected UTC date in the lookback window.
    """

    now = datetime.now(UTC)
    start = now - timedelta(hours=lookback_hours)
    days: set[str] = set()
    cur = start.date()
    while cur <= now.date():
        days.add(cur.isoformat())
        cur = (datetime.combine(cur, datetime.min.time(), tzinfo=UTC) + timedelta(days=1)).date()

    for d in sorted(days):
        written = build_gold_tables(run_date=d, market_tags_path=market_tags_path)
        typer.echo(json.dumps({"date": d, **written}, indent=2, sort_keys=True))


@app.command("mature-labels")
def mature_labels(
    dt: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    market_tags_path: str = typer.Option(
        DEFAULT_MARKET_TAGS_PATH,
        "--market-tags-path",
        help="Path to manually curated market tags CSV.",
    ),
) -> None:
    """Rebuild gold so forward labels can mature.

    MVP behavior: recompute gold outputs for the specified date.
    """

    written = build_gold_tables(run_date=dt, market_tags_path=market_tags_path)
    typer.echo(json.dumps({"date": dt, **written}, indent=2, sort_keys=True))


@app.command("compact-partitions")
def compact_partitions(
    layer: str = typer.Option("silver", "--layer", help="raw|bronze|silver|gold"),
    dataset: str = typer.Option(..., "--dataset", help="Dataset name (e.g. crypto_candles_1m)"),
    dt: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    hour: int | None = typer.Option(
        None, "--hour", min=0, max=23, help="Hour for hourly compaction"
    ),
    to: str = typer.Option("hourly", "--to", help="hourly|daily|both"),
) -> None:
    """Compact micro-batch partitions to reduce small-file overhead."""

    if to not in {"hourly", "daily", "both"}:
        raise typer.BadParameter("to must be one of: hourly, daily, both")

    results: dict[str, object] = {"layer": layer, "dataset": dataset, "date": dt, "to": to}
    if to in {"hourly", "both"}:
        if hour is None:
            raise typer.BadParameter("--hour is required for hourly compaction")
        res = compact_hourly_microbatches(layer=layer, dataset=dataset, run_date=dt, hour=hour)
        results["hourly"] = res.__dict__ if res else None
    if to in {"daily", "both"}:
        res = compact_daily_from_hourly(layer=layer, dataset=dataset, run_date=dt)
        results["daily"] = res.__dict__ if res else None

    typer.echo(json.dumps(results, indent=2, sort_keys=True))


@dq_app.command("run")
@app.command("run-soda-checks")
def cli_run_soda_checks(
    dt: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    materialize_tables: bool = typer.Option(
        False,
        "--materialize-tables",
        help=(
            "Debug/CI fallback: materialize Parquet into DuckDB tables "
            "(default is external DuckDB views over Parquet)."
        ),
    ),
) -> None:
    summary = run_soda_checks(run_date=dt, materialize_tables=materialize_tables)
    typer.echo(json.dumps(summary, indent=2, sort_keys=True))


@issues_app.command("detect")
@app.command("detect-data-issues")
def cli_detect_data_issues(
    dt: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    market_tags_path: str = typer.Option(
        DEFAULT_MARKET_TAGS_PATH,
        "--market-tags-path",
        help="Path to manually curated market tags CSV.",
    ),
) -> None:
    issues = detect_data_issues(run_date=dt, market_tags_path=market_tags_path)
    paths = write_data_issues_reports(issues)
    typer.echo(f"Wrote {paths['md']}")
    typer.echo(f"Wrote {paths['json']}")
    typer.echo(f"issues={len(issues)}")


@issues_app.command("fast")
@app.command("fast-data-issues")
def cli_fast_data_issues(
    dt: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    market_tags_path: str = typer.Option(
        DEFAULT_MARKET_TAGS_PATH,
        "--market-tags-path",
        help="Path to manually curated market tags CSV.",
    ),
    write_reports: bool = typer.Option(
        False,
        "--write-reports",
        help="Write markdown/json reports (default: no, keep hot path fast).",
    ),
) -> None:
    """Run lightweight issue detection suitable for frequent schedules."""

    issues = detect_data_issues(run_date=dt, market_tags_path=market_tags_path)
    out: dict[str, object] = {"date": dt, "issues": issues, "issues_count": len(issues)}
    if write_reports:
        out["report_paths"] = write_data_issues_reports(issues)
    typer.echo(json.dumps(out, indent=2, sort_keys=True))


@dagster_app.command("dev")
def dagster_dev() -> None:
    """Print the module path to run `dagster dev` against."""

    typer.echo("dagster dev -m crypto_belief_pipeline.orchestration.definitions")


@dagster_app.command("materialize")
def dagster_materialize(
    select: str = typer.Option(..., "--select", help="Dagster asset selection string."),
    partition: str | None = typer.Option(
        None, "--partition", help="Partition key (e.g. YYYY-MM-DD)."
    ),
    module: str = typer.Option(
        "crypto_belief_pipeline.orchestration.definitions",
        "--module",
        help="Dagster module containing `defs`.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print the command without running it.",
    ),
) -> None:
    """Wrapper around `dagster asset materialize` for this repo."""

    cmd: list[str] = ["dagster", "asset", "materialize", "-m", module, "--select", select]
    if partition:
        cmd.extend(["--partition", partition])

    if dry_run:
        typer.echo(" ".join(cmd))
        return

    raise typer.Exit(code=subprocess.call(cmd))


@app.command("generate-reports")
def cli_generate_reports(
    dt: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
) -> None:
    """Generate `reports/index.md` (same layout as Dagster ``markdown_reports``)."""

    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    index_path = reports_dir / "index.md"

    issues_count = load_issues_count_from_disk(reports_dir / "data_issues.json")
    soda_passed = load_soda_passed_from_disk(reports_dir / "soda_scan_summary.json")

    body = render_reports_index_md(
        dt,
        issues_count=issues_count,
        soda_passed=soda_passed,
        soda_paths=None,
    )
    index_path.write_text(body, encoding="utf-8")
    typer.echo(f"Wrote {index_path}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
