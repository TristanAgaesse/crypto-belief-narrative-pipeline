from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from crypto_belief_pipeline.collectors.http import get_json

KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
PING_URL = "https://fapi.binance.com/fapi/v1/ping"

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _ms_to_iso_utc(ms: Any) -> str:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=UTC).strftime(_TS_FORMAT)


def fetch_klines(
    symbol: str,
    interval: str = "1m",
    limit: int = 120,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
) -> list[list]:
    """Fetch USD-M futures klines from Binance.

    Returns the raw array-of-arrays response (each inner list is one kline).
    """

    params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = int(start_time_ms)
    if end_time_ms is not None:
        params["endTime"] = int(end_time_ms)

    payload = get_json(KLINES_URL, params=params)
    if isinstance(payload, dict):
        # Error JSON: {"code": int, "msg": str} — treat as no klines.
        return []
    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected Binance klines response shape: {type(payload).__name__}")
    return [k for k in payload if isinstance(k, list)]


def to_raw_kline_record(kline: list, symbol: str, interval: str) -> dict:
    """Convert a Binance kline array into the Step 2 raw klines contract.

    Binance kline array indices:
      0 open_time (ms)
      1 open
      2 high
      3 low
      4 close
      5 volume
      6 close_time (ms)
      7 quote_asset_volume
      8 number_of_trades
    """

    if len(kline) < 9:
        raise ValueError(f"Binance kline array too short: {len(kline)} entries")

    return {
        "open_time": _ms_to_iso_utc(kline[0]),
        "symbol": symbol,
        "interval": interval,
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "volume": float(kline[5]),
        "close_time": _ms_to_iso_utc(kline[6]),
        "quote_volume": float(kline[7]),
        "number_of_trades": int(kline[8]),
        "raw": list(kline),
    }


def collect_binance_raw(
    symbols: list[str] | None = None,
    interval: str = "1m",
    limit: int = 120,
) -> list[dict]:
    """Collect klines for the given symbols and return raw records."""

    syms = list(symbols) if symbols else list(DEFAULT_SYMBOLS)
    out: list[dict] = []
    for symbol in syms:
        klines = fetch_klines(symbol=symbol, interval=interval, limit=limit)
        for k in klines:
            try:
                out.append(to_raw_kline_record(k, symbol=symbol, interval=interval))
            except (ValueError, TypeError):
                continue
    return out
