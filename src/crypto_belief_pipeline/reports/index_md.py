from __future__ import annotations

from pathlib import Path
from typing import Any

DEFAULT_SODA_OUTPUT_TXT = "reports/soda_scan_output.txt"
DEFAULT_SODA_SUMMARY_JSON = "reports/soda_scan_summary.json"
DEFAULT_ISSUES_MD = "reports/data_issues.md"
DEFAULT_ISSUES_JSON = "reports/data_issues.json"


def reports_dir_for_partition(run_date: str, partition_key: str | None = None) -> Path:
    """Return the report directory for a CLI run or a Dagster partition."""

    if not partition_key:
        return Path("reports")
    hour = _hour_from_partition_key(partition_key)
    if hour is None:
        safe_partition = partition_key.replace("/", "_").replace(":", "-")
        return Path("reports") / f"date={run_date}" / f"partition={safe_partition}"
    return Path("reports") / f"date={run_date}" / f"hour={hour}"


def _hour_from_partition_key(partition_key: str) -> str | None:
    if len(partition_key) >= 16 and partition_key[-3:] == ":00":
        hour = partition_key[-5:-3]
        if hour.isdigit():
            return hour
    if "T" in partition_key:
        time_part = partition_key.rsplit("T", 1)[1]
        hour = time_part[:2]
        if hour.isdigit():
            return hour
    return None


def render_reports_index_md(
    run_date: str,
    *,
    issues_count: int | None = None,
    soda_passed: bool | None = None,
    soda_paths: dict[str, str] | None = None,
    issues_paths: dict[str, str] | None = None,
) -> str:
    """Build the canonical `reports/index.md` body used by Dagster and the CLI."""

    sp = soda_paths or {}
    ip = issues_paths or {}
    output_txt = sp.get("output_txt", DEFAULT_SODA_OUTPUT_TXT)
    summary_json = sp.get("summary_json", DEFAULT_SODA_SUMMARY_JSON)
    issues_md = ip.get("md", DEFAULT_ISSUES_MD)
    issues_json = ip.get("json", DEFAULT_ISSUES_JSON)

    lines: list[str] = [
        f"# Reports ({run_date})",
        "",
        "## Pipeline run summary",
        "- JSON: `reports/run_summary.json`",
        "- Markdown: `reports/run_summary.md`",
        "",
        "## Data quality (Soda)",
        f"- Output: `{output_txt}`",
        f"- Summary: `{summary_json}`",
        "",
        "## Data issues (domain detector)",
        f"- Markdown: `{issues_md}`",
        f"- JSON: `{issues_json}`",
        "",
    ]

    if issues_count is not None or soda_passed is not None:
        lines.extend(
            [
                "## Quick stats",
                "",
            ]
        )
        if issues_count is not None:
            lines.append(f"- Issues: {issues_count}")
        if soda_passed is not None:
            lines.append(f"- Soda passed: {soda_passed}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _safe_json_load(path: Path) -> Any:
    try:
        import json

        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_issues_count_from_disk(issues_json: Path | None = None) -> int | None:
    """Best-effort count of issues from `reports/data_issues.json` (list of dicts)."""

    p = issues_json or Path(DEFAULT_ISSUES_JSON)
    data = _safe_json_load(p)
    if isinstance(data, list):
        return len(data)
    return None


def load_soda_passed_from_disk(summary_json: Path | None = None) -> bool | None:
    """Best-effort `passed` flag from `reports/soda_scan_summary.json`."""

    p = summary_json or Path(DEFAULT_SODA_SUMMARY_JSON)
    data = _safe_json_load(p)
    if isinstance(data, dict) and "passed" in data:
        return bool(data["passed"])
    return None
