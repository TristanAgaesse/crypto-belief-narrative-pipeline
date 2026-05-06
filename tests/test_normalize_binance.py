import polars as pl

from crypto_belief_pipeline.sample_data import load_sample_jsonl
from crypto_belief_pipeline.transform.normalize_binance import (
    normalize_klines,
    to_crypto_candles_1m,
)


def test_normalize_klines_schema_and_exchange() -> None:
    records = load_sample_jsonl("binance_klines_sample.jsonl")
    df = normalize_klines(records)
    assert df.height >= 18
    assert df.schema["timestamp"] == pl.Datetime
    assert df["exchange"].unique().to_list() == ["binance_usdm"]


def test_to_crypto_candles_1m_asset_mapping() -> None:
    records = load_sample_jsonl("binance_klines_sample.jsonl")
    bronze = normalize_klines(records)
    silver = to_crypto_candles_1m(bronze)
    assets = set(silver["asset"].unique().to_list())
    assert assets == {"BTC", "ETH", "SOL"}
