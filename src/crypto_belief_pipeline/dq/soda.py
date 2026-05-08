from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from crypto_belief_pipeline.dq.duckdb_views import create_duckdb_quality_db, open_quality_connection


def _as_date_str(run_date: date | str) -> str:
    return run_date.isoformat() if isinstance(run_date, date) else str(run_date)


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def run_soda_checks(
    run_date: date | str,
    config_path: str | Path = "dq/configuration.yml",
    checks_dir: str | Path = "dq/checks",
    db_path: str | Path = "data/quality/crypto_lake.duckdb",
    *,
    materialize_tables: bool = False,
    bucket: str | None = None,
    partition_key: str | None = None,
    reports_dir: str | Path = "reports",
) -> dict[str, Any]:
    """Run Soda Core checks against a DuckDB quality DB for the given run_date.

    Default behavior is external Parquet views (no duplication). For CI/debug, callers can
    set materialize_tables=True.

    ``bucket`` lets sample-mode callers point views at the dedicated sample
    bucket. Leave unset for live runs (uses ``s3_bucket`` from settings).
    """

    rd = _as_date_str(run_date)
    config_path = Path(config_path)
    checks_dir = Path(checks_dir)
    db_path = Path(db_path)

    create_duckdb_quality_db(
        rd,
        db_path=db_path,
        materialize_tables=materialize_tables,
        bucket=bucket,
        partition_key=partition_key,
    )

    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_txt = reports_dir / "soda_scan_output.txt"
    out_json = reports_dir / "soda_scan_summary.json"

    try:
        from soda.scan import Scan  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Soda Core python API not available. Ensure soda-core-duckdb is installed."
        ) from e

    scan = Scan()
    scan.set_data_source_name("crypto_lake")
    scan.add_configuration_yaml_file(str(config_path))
    # Ensure Soda uses a DuckDB connection with httpfs + MinIO/S3 configured from `.env`.
    duck_con = open_quality_connection(db_path)
    scan.add_duckdb_connection(duck_con, data_source_name="crypto_lake")

    check_files = sorted(p for p in checks_dir.glob("*.yml") if p.is_file())
    for p in check_files:
        scan.add_sodacl_yaml_file(str(p))

    try:
        scan.execute()
    finally:
        try:
            duck_con.close()
        except Exception:
            pass

    logs_text = scan.get_logs_text()
    _ensure_parent(out_txt)
    out_txt.write_text(logs_text, encoding="utf-8")

    # Normalize failed checks from scan results (best-effort; Soda result structure can evolve)
    scan_results = scan.get_scan_results() or {}
    checks = scan_results.get("checks") if isinstance(scan_results, dict) else None
    failed_checks: list[dict[str, Any]] = []
    if isinstance(checks, list):
        for c in checks:
            if not isinstance(c, dict):
                continue
            outcome = c.get("outcome") or c.get("outcome_text") or c.get("status")
            if str(outcome).lower() in {"fail", "failed", "error"}:
                failed_checks.append(c)

    passed = not scan.has_error_logs()
    if failed_checks or scan.has_error_logs():
        passed = False

    summary: dict[str, Any] = {
        "run_date": rd,
        "passed": bool(passed),
        "has_errors": bool(scan.has_error_logs()),
        "failed_checks": failed_checks,
        "report_paths": {"output_txt": str(out_txt), "summary_json": str(out_json)},
    }

    _ensure_parent(out_json)
    out_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary
