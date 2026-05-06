from datetime import date

import typer

from crypto_belief_pipeline.config import get_settings
from crypto_belief_pipeline.lake.paths import partition_path
from crypto_belief_pipeline.lake.s3 import ensure_bucket_exists
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


def main() -> None:
    app()


if __name__ == "__main__":
    main()
