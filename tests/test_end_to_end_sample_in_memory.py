from __future__ import annotations

import io
from pathlib import Path

import polars as pl

from crypto_belief_pipeline.features.build_gold import build_gold_tables
from crypto_belief_pipeline.transform.run_sample_pipeline import run_sample_pipeline


class _InMemoryS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, Bucket: str, Key: str, Body: bytes, **_kwargs) -> None:  # noqa: N803
        self.objects[(Bucket, Key)] = Body

    def get_object(self, Bucket: str, Key: str, **_kwargs):  # noqa: N803, ANN001
        body = self.objects[(Bucket, Key)]
        return {"Body": io.BytesIO(body)}

    def head_bucket(self, Bucket: str) -> None:  # noqa: N803
        return None

    def create_bucket(self, Bucket: str) -> None:  # noqa: N803
        return None


def test_end_to_end_sample_pipeline_writes_gold_in_memory(monkeypatch, tmp_path: Path) -> None:
    # Configure settings via env vars (used by lake read/write helpers).
    import crypto_belief_pipeline.config as cfg

    cfg.get_settings.cache_clear()
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("S3_BUCKET", "test-bucket")

    # Enable sample mode without writing to real S3/MinIO. Sample isolation is
    # bucket-based, so we configure a dedicated sample bucket here.
    import crypto_belief_pipeline.io_guardrails as ig

    sample_bucket_name = "test-bucket-sample"

    class _Sample:
        sample_enabled = True
        sample_lake_bucket = sample_bucket_name

    class _Runtime:
        sample = _Sample()

    # The shared guardrail looks up runtime + settings via io_guardrails; patch
    # both there so the validator sees a sample bucket distinct from "test-bucket".
    monkeypatch.setattr(ig, "get_runtime_config", lambda: _Runtime())

    # Wire lake I/O to an in-memory fake S3 client (handles multi-bucket).
    fake = _InMemoryS3()
    monkeypatch.setattr(
        "crypto_belief_pipeline.lake.s3.get_s3_client",
        lambda settings=None: fake,
    )
    monkeypatch.setattr(
        "crypto_belief_pipeline.lake.read.get_s3_client",
        lambda settings=None: fake,
    )
    monkeypatch.setattr(
        "crypto_belief_pipeline.lake.write.get_s3_client",
        lambda settings=None: fake,
    )

    written = run_sample_pipeline("2026-05-06")
    assert written["sample_bucket"] == sample_bucket_name
    # Sample writes use canonical key layout (no `__sample__` prefix).
    assert written["silver_belief_price_snapshots"].startswith("silver/")

    # All sample objects must live in the sample bucket, not the live one.
    bucket_names = {b for b, _ in fake.objects}
    assert bucket_names == {sample_bucket_name}

    tags_csv = tmp_path / "market_tags.csv"
    tags_csv.write_text(
        "market_id,asset,narrative,direction,relevance,confidence,notes\n"
        "pm_btc_reserve_001,BTC,bitcoin_reserve,1,high,0.9,sample\n"
        "pm_eth_etf_staking_001,ETH,eth_etf_staking,1,high,0.9,sample\n",
        encoding="utf-8",
    )

    gold_written = build_gold_tables(
        run_date="2026-05-06",
        belief_key=written["silver_belief_price_snapshots"],
        candles_key=written["silver_crypto_candles_1m"],
        narrative_key=written["silver_narrative_counts"],
        market_tags_path=tags_csv,
        bucket=sample_bucket_name,
    )
    assert set(gold_written.keys()) == {"training_examples", "live_signals"}

    from crypto_belief_pipeline.lake.read import read_parquet_df

    training = read_parquet_df(gold_written["training_examples"], bucket=sample_bucket_name)
    assert isinstance(training, pl.DataFrame)
    assert training.height >= 1

    # Live bucket must remain empty: gold writes were routed to the sample bucket.
    live_objects = {key for (b, key) in fake.objects if b == "test-bucket"}
    assert live_objects == set()
