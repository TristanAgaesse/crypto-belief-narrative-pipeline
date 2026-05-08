from __future__ import annotations

from functools import lru_cache

import yaml  # type: ignore[import-untyped]
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    env: str = "local"
    aws_endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str = "us-east-1"
    s3_bucket: str
    s3_prefix: str = ""
    # Kalshi Trade API (public market data works without credentials).
    kalshi_trade_api_base: str = "https://external-api.kalshi.com/trade-api/v2"


class RuntimeCadence(BaseModel):
    binance_raw_seconds: int = 60
    polymarket_prices_seconds: int = 300
    polymarket_discovery_seconds: int = 21600
    kalshi_raw_seconds: int = 300
    gdelt_seconds: int = 3600
    silver_seconds: int = 300
    gold_live_seconds: int = 300
    gold_labels_seconds: int = 3600
    soda_seconds: int = 3600
    reports_seconds: int = 86400


class RuntimeQuality(BaseModel):
    fast_checks_enabled: bool = True
    soda_enabled: bool = True
    soda_on_every_run: bool = False


class RuntimeLake(BaseModel):
    micro_batch: bool = True
    partition_granularity: str = "hour"
    compact_hourly: bool = True
    compact_daily: bool = True


class RuntimeSampleIO(BaseModel):
    """Sample-mode I/O settings.

    Sample isolation is bucket-based: all sample writes/reads go to a dedicated
    ``sample_lake_bucket`` using the **same** key layout as live runs
    (``raw/...``, ``silver/...``, ``gold/...``). This avoids per-key prefix
    plumbing and matches typical environment-per-bucket conventions.
    """

    sample_enabled: bool = False
    sample_lake_bucket: str | None = None


class RuntimeConfig(BaseModel):
    mode: str = Field(default="low_latency")
    cadence: RuntimeCadence = Field(default_factory=RuntimeCadence)
    lake: RuntimeLake = Field(default_factory=RuntimeLake)
    quality: RuntimeQuality = Field(default_factory=RuntimeQuality)
    sample: RuntimeSampleIO = Field(default_factory=RuntimeSampleIO)


def _read_yaml(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


@lru_cache(maxsize=1)
def get_runtime_config(path: str = "config/runtime.yaml") -> RuntimeConfig:
    data = _read_yaml(path)
    return RuntimeConfig.model_validate(data)
