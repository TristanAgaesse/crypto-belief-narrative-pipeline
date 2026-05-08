from __future__ import annotations

import json

import pytest

import crypto_belief_pipeline.cli as cli


def _silence_typer_echo(monkeypatch) -> None:
    monkeypatch.setattr(cli.typer, "echo", lambda *_a, **_kw: None)


def _patch_sample_bucket(monkeypatch, name: str = "sample-bucket") -> None:
    monkeypatch.setattr(cli, "_ensure_bucket", lambda: None)
    monkeypatch.setattr(cli, "_ensure_sample_bucket", lambda: name)


def test_pipeline_run_sample_threads_silver_keys_into_build_gold(monkeypatch) -> None:
    """Sample mode must pass the sample silver keys + bucket to gold.

    Without this, gold reads/writes hit the live bucket or use stale defaults.
    """

    _silence_typer_echo(monkeypatch)
    _patch_sample_bucket(monkeypatch)

    sample_silver = {
        "silver_belief_price_snapshots": (
            "silver/belief_price_snapshots/date=2026-05-06/data.parquet"
        ),
        "silver_crypto_candles_1m": "silver/crypto_candles_1m/date=2026-05-06/data.parquet",
        "silver_narrative_counts": "silver/narrative_counts/date=2026-05-06/data.parquet",
        "sample_bucket": "sample-bucket",
    }

    monkeypatch.setattr(cli, "run_sample_pipeline", lambda run_date: sample_silver)

    captured: dict[str, object] = {}

    def fake_build_gold(
        *, run_date, belief_key=None, candles_key=None, narrative_key=None, bucket=None, **_
    ):
        captured["run_date"] = run_date
        captured["belief_key"] = belief_key
        captured["candles_key"] = candles_key
        captured["narrative_key"] = narrative_key
        captured["bucket"] = bucket
        return {
            "training_examples": "gold/training_examples/date=2026-05-06/data.parquet",
            "live_signals": "gold/live_signals/date=2026-05-06/data.parquet",
        }

    monkeypatch.setattr(cli, "build_gold_tables", fake_build_gold)

    monkeypatch.setattr(cli, "run_soda_checks", lambda **_kw: {"passed": True})
    monkeypatch.setattr(cli, "detect_data_issues", lambda **_kw: [])
    monkeypatch.setattr(cli, "write_data_issues_reports", lambda issues: {"md": "", "json": ""})

    cli.pipeline_run(
        dt="2026-05-06",
        mode="sample",
        sources="all",
        skip_gold=False,
        skip_dq=True,
        skip_issues=True,
    )

    assert captured["run_date"] == "2026-05-06"
    assert captured["belief_key"] == sample_silver["silver_belief_price_snapshots"]
    assert captured["candles_key"] == sample_silver["silver_crypto_candles_1m"]
    assert captured["narrative_key"] == sample_silver["silver_narrative_counts"]
    assert captured["bucket"] == "sample-bucket"


def test_pipeline_run_sample_routes_dq_and_issues_to_sample_bucket(monkeypatch) -> None:
    """Sample mode must thread sample bucket through DQ + issues."""

    _silence_typer_echo(monkeypatch)
    _patch_sample_bucket(monkeypatch, name="sample-bucket")

    sample_silver = {
        "silver_belief_price_snapshots": (
            "silver/belief_price_snapshots/date=2026-05-06/data.parquet"
        ),
        "silver_crypto_candles_1m": "silver/crypto_candles_1m/date=2026-05-06/data.parquet",
        "silver_narrative_counts": "silver/narrative_counts/date=2026-05-06/data.parquet",
        "sample_bucket": "sample-bucket",
    }
    monkeypatch.setattr(cli, "run_sample_pipeline", lambda run_date: sample_silver)
    monkeypatch.setattr(cli, "build_gold_tables", lambda **_kw: {"training_examples": "g.parquet"})

    soda_calls: list[dict[str, object]] = []
    issues_calls: list[dict[str, object]] = []

    def fake_soda(**kwargs):
        soda_calls.append(kwargs)
        return {"passed": True}

    def fake_issues(**kwargs):
        issues_calls.append(kwargs)
        return []

    monkeypatch.setattr(cli, "run_soda_checks", fake_soda)
    monkeypatch.setattr(cli, "detect_data_issues", fake_issues)
    monkeypatch.setattr(cli, "write_data_issues_reports", lambda issues: {"md": "", "json": ""})

    cli.pipeline_run(
        dt="2026-05-06",
        mode="sample",
        sources="all",
        skip_gold=False,
        skip_dq=False,
        skip_issues=False,
    )

    assert len(soda_calls) == 1
    assert soda_calls[0]["bucket"] == "sample-bucket"
    assert len(issues_calls) == 1
    assert issues_calls[0]["bucket"] == "sample-bucket"


def test_pipeline_run_live_partial_sources_uses_only_selected(monkeypatch) -> None:
    """`--sources binance` must NOT trigger normalize for skipped sources."""

    _silence_typer_echo(monkeypatch)
    monkeypatch.setattr(cli, "_ensure_bucket", lambda: None)

    raw_written = {"raw_binance_klines": "raw/provider=binance/date=2026-05-06/live_klines.jsonl"}

    def fake_run_live_collectors(**kwargs):
        # Only binance was requested.
        assert kwargs["collect_binance"] is True
        assert kwargs["collect_polymarket"] is False
        assert kwargs["collect_gdelt"] is False
        return raw_written

    monkeypatch.setattr(cli, "run_live_collectors", fake_run_live_collectors)

    captured_sources: list[set[str]] = []

    def fake_run_live_pipeline(*, run_date, sources=None, **_kw):
        captured_sources.append(set(sources) if sources is not None else set())
        return {"silver_crypto_candles_1m": "silver/crypto_candles_1m/date=2026-05-06/data.parquet"}

    monkeypatch.setattr(cli, "run_live_pipeline", fake_run_live_pipeline)
    monkeypatch.setattr(cli, "build_gold_tables", lambda **_kw: {"training_examples": "x"})

    cli.pipeline_run(
        dt="2026-05-06",
        mode="live",
        sources="binance",
        skip_gold=True,
        skip_dq=True,
        skip_issues=True,
    )

    assert captured_sources == [{"binance"}]


def test_pipeline_run_rejects_unknown_mode(monkeypatch) -> None:
    _silence_typer_echo(monkeypatch)
    monkeypatch.setattr(cli, "_ensure_bucket", lambda: None)

    with pytest.raises(cli.typer.BadParameter):
        cli.pipeline_run(
            dt="2026-05-06",
            mode="batch",
            sources="all",
            skip_gold=True,
            skip_dq=True,
            skip_issues=True,
        )


def test_pipeline_run_live_polymarket_binance_runs_gold_without_gdelt(monkeypatch) -> None:
    """Polymarket + Binance without GDELT can still build gold (GDELT optional)."""

    _silence_typer_echo(monkeypatch)
    monkeypatch.setattr(cli, "_ensure_bucket", lambda: None)

    monkeypatch.setattr(cli, "run_live_collectors", lambda **_kw: {})

    def fake_run_live_pipeline(*, run_date, sources=None, **_kw):
        assert sources == {"polymarket", "binance"}
        return {
            "silver_belief_price_snapshots": (
                "silver/belief_price_snapshots/date=2026-05-06/data.parquet"
            ),
            "silver_crypto_candles_1m": "silver/crypto_candles_1m/date=2026-05-06/data.parquet",
            "silver_narrative_counts": "silver/narrative_counts/date=2026-05-06/data.parquet",
        }

    monkeypatch.setattr(cli, "run_live_pipeline", fake_run_live_pipeline)

    gold_calls: list[dict[str, object]] = []

    def fake_build_gold(**kwargs):
        gold_calls.append(kwargs)
        return {"training_examples": "g", "live_signals": "l"}

    monkeypatch.setattr(cli, "build_gold_tables", fake_build_gold)

    cli.pipeline_run(
        dt="2026-05-06",
        mode="live",
        sources="polymarket,binance",
        skip_gold=False,
        skip_dq=True,
        skip_issues=True,
    )

    assert len(gold_calls) == 1
    assert gold_calls[0]["belief_key"] == (
        "silver/belief_price_snapshots/date=2026-05-06/data.parquet"
    )
    assert gold_calls[0]["candles_key"] == "silver/crypto_candles_1m/date=2026-05-06/data.parquet"
    assert gold_calls[0]["narrative_key"] == "silver/narrative_counts/date=2026-05-06/data.parquet"


def test_pipeline_run_live_binance_only_still_fails_without_polymarket(monkeypatch) -> None:
    """Gold still requires Polymarket silver; Binance-only partial runs must fail."""

    _silence_typer_echo(monkeypatch)
    monkeypatch.setattr(cli, "_ensure_bucket", lambda: None)

    def _boom(*_a, **_kw):
        raise AssertionError("downstream call should not happen when guard fires")

    monkeypatch.setattr(cli, "run_live_collectors", _boom)
    monkeypatch.setattr(cli, "run_live_pipeline", _boom)
    monkeypatch.setattr(cli, "build_gold_tables", _boom)

    with pytest.raises(cli.typer.BadParameter, match="Missing required"):
        cli.pipeline_run(
            dt="2026-05-06",
            mode="live",
            sources="binance",
            skip_gold=False,
            skip_dq=True,
            skip_issues=True,
        )


def test_pipeline_run_live_partial_sources_allowed_when_all_downstream_skipped(monkeypatch) -> None:
    """Partial `--sources` is allowed when --skip-gold --skip-dq --skip-issues are all set."""

    _silence_typer_echo(monkeypatch)
    monkeypatch.setattr(cli, "_ensure_bucket", lambda: None)
    monkeypatch.setattr(cli, "run_live_collectors", lambda **_kw: {})
    monkeypatch.setattr(cli, "run_live_pipeline", lambda **_kw: {})

    def _boom(*_a, **_kw):
        raise AssertionError("downstream call should not happen when all skipped")

    monkeypatch.setattr(cli, "build_gold_tables", _boom)
    monkeypatch.setattr(cli, "run_soda_checks", _boom)
    monkeypatch.setattr(cli, "detect_data_issues", _boom)

    cli.pipeline_run(
        dt="2026-05-06",
        mode="live",
        sources="binance",
        skip_gold=True,
        skip_dq=True,
        skip_issues=True,
    )


def test_pipeline_run_live_full_sources_threads_silver_keys_to_gold(monkeypatch) -> None:
    """In live mode, gold/DQ/issues must receive silver keys from the live normalize step.

    Falling back to default keys would re-introduce the stale-mix risk this guard removes.
    """

    _silence_typer_echo(monkeypatch)
    monkeypatch.setattr(cli, "_ensure_bucket", lambda: None)
    monkeypatch.setattr(cli, "run_live_collectors", lambda **_kw: {})

    normalized = {
        "silver_belief_price_snapshots": (
            "silver/belief_price_snapshots/date=2026-05-06/data.parquet"
        ),
        "silver_crypto_candles_1m": "silver/crypto_candles_1m/date=2026-05-06/data.parquet",
        "silver_narrative_counts": "silver/narrative_counts/date=2026-05-06/data.parquet",
        "__sources_processed__": "binance,gdelt,polymarket",
        "__sources_skipped__": "",
    }
    monkeypatch.setattr(cli, "run_live_pipeline", lambda **_kw: normalized)

    captured: dict[str, object] = {}

    def fake_build_gold(
        *, run_date, belief_key=None, candles_key=None, narrative_key=None, bucket=None, **_
    ):
        captured["belief_key"] = belief_key
        captured["candles_key"] = candles_key
        captured["narrative_key"] = narrative_key
        captured["bucket"] = bucket
        return {"training_examples": "g", "live_signals": "l"}

    monkeypatch.setattr(cli, "build_gold_tables", fake_build_gold)

    soda_calls: list[dict[str, object]] = []
    issues_calls: list[dict[str, object]] = []
    monkeypatch.setattr(cli, "run_soda_checks", lambda **kw: soda_calls.append(kw) or {})
    monkeypatch.setattr(cli, "detect_data_issues", lambda **kw: issues_calls.append(kw) or [])
    monkeypatch.setattr(cli, "write_data_issues_reports", lambda issues: {"md": "", "json": ""})

    cli.pipeline_run(
        dt="2026-05-06",
        mode="live",
        sources="all",
        skip_gold=False,
        skip_dq=False,
        skip_issues=False,
    )

    assert captured["belief_key"] == normalized["silver_belief_price_snapshots"]
    assert captured["candles_key"] == normalized["silver_crypto_candles_1m"]
    assert captured["narrative_key"] == normalized["silver_narrative_counts"]
    # Live mode keeps the lake bucket default; we explicitly pass bucket=None.
    assert captured["bucket"] is None

    assert len(soda_calls) == 1
    assert soda_calls[0]["bucket"] is None
    assert len(issues_calls) == 1
    assert issues_calls[0]["bucket"] is None


def test_pipeline_run_emits_json_for_sample_silver_keys(monkeypatch, capsys) -> None:
    """Smoke-check: sample-mode echo carries silver keys + bucket for operator visibility."""

    _patch_sample_bucket(monkeypatch, name="sample-bucket")

    sample_silver = {
        "silver_belief_price_snapshots": "silver/foo/data.parquet",
        "silver_crypto_candles_1m": "silver/bar/data.parquet",
        "silver_narrative_counts": "silver/baz/data.parquet",
        "sample_bucket": "sample-bucket",
    }
    monkeypatch.setattr(cli, "run_sample_pipeline", lambda run_date: sample_silver)
    monkeypatch.setattr(cli, "build_gold_tables", lambda **_kw: {"training_examples": "g.parquet"})

    cli.pipeline_run(
        dt="2026-05-06",
        mode="sample",
        sources="all",
        skip_gold=False,
        skip_dq=True,
        skip_issues=True,
    )
    out = capsys.readouterr().out
    decoder = json.JSONDecoder()
    parsed, _ = decoder.raw_decode(out.lstrip())
    assert parsed["silver_belief_price_snapshots"].startswith("silver/")
    assert parsed["sample_bucket"] == "sample-bucket"
