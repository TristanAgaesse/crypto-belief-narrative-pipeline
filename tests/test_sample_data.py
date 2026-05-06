from crypto_belief_pipeline.sample_data import load_sample_jsonl


def test_load_sample_jsonl_smoke() -> None:
    records = load_sample_jsonl("polymarket_markets_sample.jsonl")
    assert len(records) == 3
    assert records[0]["market_id"].startswith("pm_")
