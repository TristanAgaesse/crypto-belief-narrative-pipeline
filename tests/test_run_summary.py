from __future__ import annotations

from crypto_belief_pipeline.reports.run_summary import (
    build_pipeline_run_summary,
    render_run_summary_markdown,
    summarize_issues_by_severity,
    write_pipeline_run_summary,
)


def test_summarize_issues_by_severity_counts() -> None:
    issues = [
        {"severity": "critical", "issue": "a"},
        {"severity": "warning", "issue": "b"},
        {"severity": "warning", "issue": "c"},
    ]
    assert summarize_issues_by_severity(issues) == {"critical": 1, "warning": 2}


def test_write_pipeline_run_summary_writes_json_and_md(tmp_path) -> None:
    summary = build_pipeline_run_summary(
        run_date="2026-05-06",
        mode="sample",
        duration_seconds=1.234,
        started_at="t0",
        finished_at="t1",
        stages={"gold": {"skipped": True}},
        warnings=["w1"],
    )
    paths = write_pipeline_run_summary(
        summary, json_path=tmp_path / "rs.json", md_path=tmp_path / "rs.md"
    )
    assert (tmp_path / "rs.json").exists()
    assert (tmp_path / "rs.md").exists()
    assert paths["json"].endswith("rs.json")
    body = render_run_summary_markdown(summary)
    assert "sample" in body
    assert "w1" in body
