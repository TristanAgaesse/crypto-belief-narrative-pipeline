from __future__ import annotations

import polars as pl
import pytest

import crypto_belief_pipeline.transform.pipeline_steps as ps
import crypto_belief_pipeline.transform.run_live_pipeline as rlp


@pytest.fixture
def fake_io(monkeypatch):
    """Patch read/write/normalize calls to focus tests on source-selection semantics."""

    writes: dict[str, pl.DataFrame] = {}
    reads_called: list[str] = []
    bronze_stub = pl.DataFrame({"x": [1]})
    # Silver stubs must satisfy the contracts in pipeline_steps' validate calls.
    silver_belief = pl.DataFrame(
        {"timestamp": [], "platform": [], "market_id": [], "outcome": [], "price": []}
    )
    silver_candles = pl.DataFrame({"timestamp": [], "exchange": [], "asset": [], "close": []})
    silver_counts = pl.DataFrame(
        {"timestamp": [], "narrative": [], "query": [], "mention_volume": [], "source": []}
    )

    def fake_read(key: str, bucket=None):
        reads_called.append(key)
        return [{"k": key}]

    def fake_write(df, key: str, bucket=None):  # noqa: ANN001
        writes[key] = df

    # `read_jsonl_records` is the only IO call still made directly by rlp; the
    # bronze/silver writes happen inside the shared pipeline_steps module.
    monkeypatch.setattr(rlp, "read_jsonl_records", fake_read)
    monkeypatch.setattr(ps, "write_parquet_df", fake_write)
    monkeypatch.setattr(ps, "normalize_markets", lambda recs: bronze_stub)
    monkeypatch.setattr(ps, "normalize_price_snapshots", lambda recs: bronze_stub)
    monkeypatch.setattr(ps, "to_belief_price_snapshots", lambda df: silver_belief)
    monkeypatch.setattr(ps, "normalize_klines", lambda recs: bronze_stub)
    monkeypatch.setattr(ps, "to_crypto_candles_1m", lambda df: silver_candles)
    monkeypatch.setattr(ps, "normalize_timeline", lambda recs: bronze_stub)
    monkeypatch.setattr(ps, "to_narrative_counts", lambda df: silver_counts)

    return writes, reads_called


def test_run_live_pipeline_processes_all_sources_by_default(fake_io) -> None:
    writes, reads = fake_io
    out = rlp.run_live_pipeline(run_date="2026-05-06")

    assert "silver_belief_price_snapshots" in out
    assert "silver_crypto_candles_1m" in out
    assert "silver_narrative_counts" in out
    assert out["__sources_processed__"] == "binance,gdelt,polymarket"
    assert out["__sources_skipped__"] == ""


def test_run_live_pipeline_with_subset_skips_unselected_sources(fake_io) -> None:
    writes, reads = fake_io
    out = rlp.run_live_pipeline(run_date="2026-05-06", sources={"binance"})

    assert "silver_crypto_candles_1m" in out
    assert "silver_belief_price_snapshots" not in out
    # GDELT is optional: when not selected we still write empty narrative silver for this
    # run_date so downstream gold does not read stale partition data.
    assert "silver_narrative_counts" in out
    assert out["__sources_processed__"] == "binance"
    # Polymarket / GDELT raw keys must NEVER be read for skipped sources, otherwise
    # we'd silently use stale data from a previous run.
    assert not any("polymarket" in k for k in reads)
    assert not any("gdelt" in k for k in reads)


def test_run_live_pipeline_rejects_unknown_sources(fake_io) -> None:
    with pytest.raises(ValueError, match="Unknown sources"):
        rlp.run_live_pipeline(run_date="2026-05-06", sources={"twitter"})


def test_run_live_pipeline_writes_only_selected_silver(fake_io) -> None:
    writes, _ = fake_io
    rlp.run_live_pipeline(run_date="2026-05-06", sources={"polymarket"})
    written_keys = list(writes.keys())
    # Polymarket-only run must NOT produce binance artifacts; optional GDELT still
    # writes empty bronze/silver narrative for this date.
    assert all("provider=binance" not in k for k in written_keys)
    assert any("provider=polymarket" in k for k in written_keys)
    assert any("provider=gdelt" in k for k in written_keys)
    assert any("silver/belief_price_snapshots" in k for k in written_keys)
    assert any("silver/narrative_counts" in k for k in written_keys)
