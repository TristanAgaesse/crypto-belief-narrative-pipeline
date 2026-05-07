from __future__ import annotations

import json
from pathlib import Path

from crypto_belief_pipeline.reports.index_md import (
    load_issues_count_from_disk,
    load_soda_passed_from_disk,
    render_reports_index_md,
)


def test_render_reports_index_md_with_quick_stats() -> None:
    md = render_reports_index_md(
        "2026-05-06",
        issues_count=3,
        soda_passed=True,
        soda_paths={"output_txt": "reports/a.txt", "summary_json": "reports/b.json"},
    )
    assert "# Reports (2026-05-06)" in md
    assert "reports/a.txt" in md
    assert "reports/b.json" in md
    assert "## Quick stats" in md
    assert "- Issues: 3" in md
    assert "- Soda passed: True" in md
    assert "domain detector" in md


def test_render_reports_index_md_without_quick_stats() -> None:
    md = render_reports_index_md("2026-05-06")
    assert "## Quick stats" not in md


def test_load_issues_count_and_soda_passed(tmp_path: Path) -> None:
    issues = tmp_path / "data_issues.json"
    issues.write_text(json.dumps([{"a": 1}, {"b": 2}]), encoding="utf-8")
    assert load_issues_count_from_disk(issues) == 2

    summary = tmp_path / "soda_scan_summary.json"
    summary.write_text(json.dumps({"passed": True}), encoding="utf-8")
    assert load_soda_passed_from_disk(summary) is True


def test_cli_and_dagster_use_same_renderer_for_known_inputs() -> None:
    """Parity: `markdown_reports` and `generate-reports` both call `render_reports_index_md`."""

    md_asset = render_reports_index_md(
        "2026-05-06",
        issues_count=2,
        soda_passed=False,
        soda_paths={
            "output_txt": "reports/soda_scan_output.txt",
            "summary_json": "reports/soda_scan_summary.json",
        },
    )
    md_cli = render_reports_index_md(
        "2026-05-06",
        issues_count=2,
        soda_passed=False,
        soda_paths=None,
    )
    assert md_asset == md_cli
