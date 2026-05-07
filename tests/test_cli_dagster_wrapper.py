from __future__ import annotations

import crypto_belief_pipeline.cli as cli


def test_dagster_materialize_builds_expected_command(monkeypatch) -> None:
    captured: list[list[str]] = []

    def fake_call(cmd: list[str]) -> int:  # noqa: ANN001
        captured.append(cmd)
        return 0

    monkeypatch.setattr(cli.subprocess, "call", fake_call)

    import pytest

    with pytest.raises(cli.typer.Exit):
        cli.dagster_materialize(
            select="raw_binance,bronze_binance,silver_crypto_candles_1m",
            partition="2026-05-06",
            module="crypto_belief_pipeline.orchestration.definitions",
            dry_run=False,
        )

    assert captured == [
        [
            "dagster",
            "asset",
            "materialize",
            "-m",
            "crypto_belief_pipeline.orchestration.definitions",
            "--select",
            "raw_binance,bronze_binance,silver_crypto_candles_1m",
            "--partition",
            "2026-05-06",
        ]
    ]
