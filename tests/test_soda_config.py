from __future__ import annotations

from pathlib import Path

import yaml


def test_soda_configuration_yaml_exists_and_parses() -> None:
    p = Path("dq/configuration.yml")
    assert p.exists()
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    assert "data_source crypto_lake" in data


def test_soda_checks_exist_and_parse() -> None:
    checks_dir = Path("dq/checks")
    assert checks_dir.exists()
    files = sorted(checks_dir.glob("*.yml"))
    assert files, "expected at least one Soda checks YAML file"
    for f in files:
        data = yaml.safe_load(f.read_text(encoding="utf-8"))
        assert isinstance(data, dict)
