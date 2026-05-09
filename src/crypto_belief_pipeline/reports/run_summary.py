"""Structured pipeline run summaries (JSON + Markdown) for operators and reviewers."""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def summarize_issues_by_severity(issues: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for it in issues:
        sev = str(it.get("severity", "info"))
        counts[sev] += 1
    return dict(sorted(counts.items()))


def build_pipeline_run_summary(
    *,
    run_date: str,
    mode: str,
    duration_seconds: float,
    started_at: str,
    finished_at: str,
    stages: dict[str, Any],
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    """Return a JSON-serializable summary dict."""

    return {
        "run_date": run_date,
        "mode": mode,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round(duration_seconds, 3),
        "stages": stages,
        "warnings": warnings or [],
    }


def render_run_summary_markdown(summary: dict[str, Any]) -> str:
    """Human-readable Markdown for `reports/run_summary.md`."""

    lines: list[str] = [
        f"# Pipeline run summary ({summary.get('run_date', '')})",
        "",
        f"- **Mode**: `{summary.get('mode', '')}`",
        f"- **Started**: {summary.get('started_at', '')}",
        f"- **Finished**: {summary.get('finished_at', '')}",
        f"- **Duration (s)**: {summary.get('duration_seconds', '')}",
        "",
        "## Stages",
        "",
    ]

    stages = summary.get("stages") or {}
    if isinstance(stages, dict):
        for name in sorted(stages.keys()):
            body = stages[name]
            lines.append(f"### `{name}`")
            if isinstance(body, dict):
                for k in sorted(body.keys()):
                    v = body[k]
                    if isinstance(v, (dict, list)):
                        lines.append(f"- **{k}**:")
                        lines.append(f"```json\n{json.dumps(v, indent=2, sort_keys=True)}\n```")
                    else:
                        lines.append(f"- **{k}**: {v}")
            else:
                lines.append(f"{body}")
            lines.append("")
    else:
        lines.append("(no stage details)")
        lines.append("")

    warns = summary.get("warnings") or []
    if warns:
        lines.extend(["## Warnings", ""])
        for w in warns:
            lines.append(f"- {w}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def write_pipeline_run_summary(
    summary: dict[str, Any],
    *,
    json_path: str | Path = "reports/run_summary.json",
    md_path: str | Path = "reports/run_summary.md",
) -> dict[str, str]:
    """Write JSON + Markdown run summaries; return paths."""

    jp = Path(json_path)
    mp = Path(md_path)
    jp.parent.mkdir(parents=True, exist_ok=True)
    mp.parent.mkdir(parents=True, exist_ok=True)
    jp.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    mp.write_text(render_run_summary_markdown(summary), encoding="utf-8")
    return {"json": str(jp), "markdown": str(mp)}


__all__ = [
    "build_pipeline_run_summary",
    "render_run_summary_markdown",
    "summarize_issues_by_severity",
    "utc_now_iso",
    "write_pipeline_run_summary",
]
