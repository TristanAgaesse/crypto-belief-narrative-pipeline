from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("soda.scan")


class _DummyDuckCon:
    def close(self) -> None:
        return None


def test_run_soda_checks_invokes_scan_and_writes_reports(monkeypatch, tmp_path: Path) -> None:
    from crypto_belief_pipeline.dq import soda as soda_mod

    monkeypatch.chdir(tmp_path)

    dbp = tmp_path / "quality" / "crypto_lake.duckdb"

    def _fake_create(*_a, **_k) -> Path:
        dbp.parent.mkdir(parents=True, exist_ok=True)
        return dbp

    monkeypatch.setattr(soda_mod, "create_duckdb_quality_db", _fake_create)
    monkeypatch.setattr(soda_mod, "open_quality_connection", lambda _p: _DummyDuckCon())

    executed: list[str] = []

    class _FakeScan:
        def set_data_source_name(self, _name: str) -> None:
            return None

        def add_configuration_yaml_file(self, _path: str) -> None:
            return None

        def add_duckdb_connection(self, _con, data_source_name: str | None = None) -> None:
            return None

        def add_sodacl_yaml_file(self, _path: str) -> None:
            return None

        def execute(self) -> None:
            executed.append("execute")

        def get_logs_text(self) -> str:
            return "fake soda logs"

        def get_scan_results(self) -> dict:
            return {"checks": []}

        def has_error_logs(self) -> bool:
            return False

    monkeypatch.setattr("soda.scan.Scan", _FakeScan)

    out = soda_mod.run_soda_checks("2026-05-06", db_path=dbp)

    assert executed == ["execute"]
    assert out["passed"] is True
    assert (tmp_path / "reports" / "soda_scan_output.txt").exists()
    assert (tmp_path / "reports" / "soda_scan_summary.json").exists()


def test_run_soda_checks_marks_failed_when_scan_has_errors(monkeypatch, tmp_path: Path) -> None:
    from crypto_belief_pipeline.dq import soda as soda_mod

    monkeypatch.chdir(tmp_path)
    dbp = tmp_path / "q.duckdb"
    monkeypatch.setattr(soda_mod, "create_duckdb_quality_db", lambda *a, **k: dbp)
    monkeypatch.setattr(soda_mod, "open_quality_connection", lambda _p: _DummyDuckCon())

    class _FakeScan:
        def set_data_source_name(self, _n: str) -> None:
            return None

        def add_configuration_yaml_file(self, _p: str) -> None:
            return None

        def add_duckdb_connection(self, _c, data_source_name: str | None = None) -> None:
            return None

        def add_sodacl_yaml_file(self, _p: str) -> None:
            return None

        def execute(self) -> None:
            return None

        def get_logs_text(self) -> str:
            return "err"

        def get_scan_results(self) -> dict:
            return {"checks": [{"outcome": "fail", "name": "x"}]}

        def has_error_logs(self) -> bool:
            return True

    monkeypatch.setattr("soda.scan.Scan", _FakeScan)

    out = soda_mod.run_soda_checks("2026-05-06", db_path=dbp)
    assert out["passed"] is False
    assert out["failed_checks"]
