from __future__ import annotations

from datetime import date, timedelta

import typer

from crypto_belief_pipeline.collectors.run_live_collectors import run_live_collectors
from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.features.build_gold import build_gold_tables
from crypto_belief_pipeline.features.market_tags import DEFAULT_MARKET_TAGS_PATH
from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.s3 import ensure_bucket_exists
from crypto_belief_pipeline.transform.run_raw_to_silver import run_raw_to_silver
from crypto_belief_pipeline.transform.run_sample_pipeline import run_sample_pipeline

app = typer.Typer(add_completion=False)


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
    skip_polymarket: bool = typer.Option(False, "--skip-polymarket"),
    skip_binance: bool = typer.Option(False, "--skip-binance"),
    skip_gdelt: bool = typer.Option(False, "--skip-gdelt"),
) -> None:
    written = run_live_collectors(
        run_date=dt,
        collect_polymarket=not skip_polymarket,
        collect_binance=not skip_binance,
        collect_gdelt=not skip_gdelt,
        binance_limit=binance_limit,
        polymarket_limit=polymarket_limit,
    )
    for name in sorted(written.keys()):
        typer.echo(f"{name} {written[name]}")


@app.command("raw-to-silver")
def raw_to_silver(
    dt: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    polymarket_markets_key: str = typer.Option(..., "--polymarket-markets-key"),
    polymarket_prices_key: str = typer.Option(..., "--polymarket-prices-key"),
    binance_klines_key: str = typer.Option(..., "--binance-klines-key"),
    gdelt_timeline_key: str = typer.Option(..., "--gdelt-timeline-key"),
) -> None:
    written = run_raw_to_silver(
        run_date=dt,
        polymarket_markets_key=polymarket_markets_key,
        polymarket_prices_key=polymarket_prices_key,
        binance_klines_key=binance_klines_key,
        gdelt_timeline_key=gdelt_timeline_key,
    )
    for name in sorted(written.keys()):
        typer.echo(f"{name} {written[name]}")


@app.command("run-live")
def run_live(
    dt: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
    polymarket_limit: int = typer.Option(100, "--polymarket-limit"),
    binance_limit: int = typer.Option(120, "--binance-limit"),
) -> None:
    s = get_settings()
    ensure_bucket_exists(s.s3_bucket, settings=s)

    raw_written = run_live_collectors(
        run_date=dt,
        binance_limit=binance_limit,
        polymarket_limit=polymarket_limit,
    )

    pm_prefix = partition_path("raw", "provider=polymarket", dt)
    bn_prefix = partition_path("raw", "provider=binance", dt)
    gd_prefix = partition_path("raw", "provider=gdelt", dt)

    silver_written = run_raw_to_silver(
        run_date=dt,
        polymarket_markets_key=raw_written.get(
            "raw_polymarket_markets", f"{pm_prefix}/live_markets.jsonl"
        ),
        polymarket_prices_key=raw_written.get(
            "raw_polymarket_prices", f"{pm_prefix}/live_prices.jsonl"
        ),
        binance_klines_key=raw_written.get("raw_binance_klines", f"{bn_prefix}/live_klines.jsonl"),
        gdelt_timeline_key=raw_written.get(
            "raw_gdelt_timeline", f"{gd_prefix}/live_timeline.jsonl"
        ),
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


def main() -> None:
    app()


if __name__ == "__main__":
    main()
