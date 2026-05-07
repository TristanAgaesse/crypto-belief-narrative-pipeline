from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import typer

from crypto_belief_pipeline.collectors.run_live_collectors import run_live_collectors
from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.dq.soda import run_soda_checks
from crypto_belief_pipeline.features.build_gold import build_gold_tables
from crypto_belief_pipeline.features.market_tags import DEFAULT_MARKET_TAGS_PATH
from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.s3 import ensure_bucket_exists
from crypto_belief_pipeline.quality.issues import detect_data_issues, write_data_issues_reports
from crypto_belief_pipeline.transform.run_raw_to_silver import run_raw_to_silver
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
    _ensure_bucket()

    if mode not in {"live", "sample"}:
        raise typer.BadParameter("mode must be one of: live, sample")

    if mode == "sample":
        written = run_sample_pipeline(run_date=run_date)
        typer.echo(json.dumps(written, indent=2, sort_keys=True))
    else:
        src = _parse_sources(sources)
        raw_written = run_live_collectors(
            run_date=run_date,
            collect_polymarket=("polymarket" in src),
            collect_binance=("binance" in src),
            collect_gdelt=("gdelt" in src),
        )
        silver_written = run_raw_to_silver(
            run_date=run_date,
            polymarket_markets_key=raw_written.get("raw_polymarket_markets")
            if "polymarket" in src
            else None,
            polymarket_prices_key=raw_written.get("raw_polymarket_prices")
            if "polymarket" in src
            else None,
            binance_klines_key=raw_written.get("raw_binance_klines") if "binance" in src else None,
            gdelt_timeline_key=raw_written.get("raw_gdelt_timeline") if "gdelt" in src else None,
        )
        typer.echo(json.dumps({**raw_written, **silver_written}, indent=2, sort_keys=True))

    if not skip_gold:
        gold_written = build_gold_tables(run_date=run_date)
        typer.echo(json.dumps(gold_written, indent=2, sort_keys=True))

    if not skip_dq:
        summary = run_soda_checks(run_date=run_date)
        typer.echo(json.dumps(summary, indent=2, sort_keys=True))

    if not skip_issues:
        issues = detect_data_issues(run_date=run_date)
        paths = write_data_issues_reports(issues)
        typer.echo(json.dumps({"issues": len(issues), "paths": paths}, indent=2, sort_keys=True))


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

    silver_written = run_raw_to_silver(
        run_date=dt,
        polymarket_markets_key=raw_written.get("raw_polymarket_markets")
        if "polymarket" in src
        else None,
        polymarket_prices_key=(
            raw_written.get("raw_polymarket_prices") if "polymarket" in src else None
        ),
        binance_klines_key=raw_written.get("raw_binance_klines") if "binance" in src else None,
        gdelt_timeline_key=raw_written.get("raw_gdelt_timeline") if "gdelt" in src else None,
    )

    combined = {**raw_written, **silver_written}
    for name in sorted(combined.keys()):
        typer.echo(f"{name} {combined[name]}")


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


@dagster_app.command("dev")
def dagster_dev() -> None:
    """Print the module path to run `dagster dev` against."""

    typer.echo("dagster dev -m crypto_belief_pipeline.orchestration.definitions")


@app.command("generate-reports")
def cli_generate_reports(
    dt: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
) -> None:
    """Generate a lightweight markdown index in reports/."""

    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    index_path = reports_dir / "index.md"

    soda_out = reports_dir / "soda_scan_output.txt"
    soda_sum = reports_dir / "soda_scan_summary.json"
    issues_md = reports_dir / "data_issues.md"
    issues_json = reports_dir / "data_issues.json"

    lines = [
        f"# Reports ({dt})",
        "",
        "## Data quality (Soda)",
        f"- Output: `{soda_out}`",
        f"- Summary: `{soda_sum}`",
        "",
        "## Data issues",
        f"- Markdown: `{issues_md}`",
        f"- JSON: `{issues_json}`",
        "",
    ]
    index_path.write_text("\n".join(lines), encoding="utf-8")
    typer.echo(f"Wrote {index_path}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
