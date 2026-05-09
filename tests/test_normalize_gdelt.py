import polars as pl

from crypto_belief_pipeline.contracts import BRONZE_GDELT_TIMELINE
from crypto_belief_pipeline.sample_data import load_sample_jsonl
from crypto_belief_pipeline.transform.normalize_gdelt import normalize_timeline


def test_normalize_timeline_empty_records_matches_bronze_contract() -> None:
    df = normalize_timeline([])
    assert df.is_empty()
    assert set(df.columns) == {
        "timestamp",
        "narrative",
        "query",
        "mention_volume",
        "avg_tone",
        "source",
        "raw_json",
        "ingested_at",
        "load_timestamp",
    }
    BRONZE_GDELT_TIMELINE.validate(df)


def test_normalize_timeline_numeric_types() -> None:
    records = load_sample_jsonl("gdelt_timeline_sample.jsonl")
    df = normalize_timeline(records)
    assert df.height == 9
    assert df.schema["mention_volume"] in {pl.Float64, pl.Float32}
    assert df.schema["avg_tone"] in {pl.Float64, pl.Float32}
