# API schema notes

Live captures under `data/api_samples/`. Summary of vendor shapes vs Step 2 raw JSONL (2026-05-06 captures).

## Polymarket Gamma (`GET /markets`)

| Field | Notes |
|-------|--------|
| `id` / `conditionId` | Market id (collector accepts either) |
| `question`, `slug` | Pass through |
| `category`, `tags` | Often missing; category from first tag or null |
| `outcomes`, `outcomePrices` | Often **JSON strings** → parsed to arrays |
| `tokens` | Per-outcome bid/ask/price when present; else outcomePrices + market bid/ask/lastTrade |
| `liquidity`, `volume` | Coerced float (incl. `*Num` variants) |
| `raw` | Full object per JSONL row → `raw_json` in bronze |

**Prices JSONL:** one row per outcome; **Yes** often has bid/ask, **No** may be price-only (~half null bid/ask). Bronze `spread` = ask−bid when both set.

## Binance klines (`fapi/v1/klines`)

Array of 12 fields per candle: open time ms, OHLC strings, volume, close time, quote volume, trades, taker fields…

Raw record: ISO Z times, floats, `raw` = original array. Error payloads (`code`/`msg`) → no klines. Silver: mapped symbols e.g. `BTCUSDT` → `BTC`.

## GDELT (`/api/v2/doc/doc`, timelinevol)

- Heavy **rate limit** (~1 req/5s); collector uses spacing, retries, `Retry-After`. Query must be **URL-encoded** as one parameter (`&` → `%26`).
- **`Filters` + YAML:** multi-phrase queries split on ` OR ` so GDELT gets `("a" OR "b")`, not doubled quotes.
- Response may have junk framing → tolerant JSON load (`gdeltdoc` helpers).
- Typical frame: `timeline[]` with `series` + `data` `[{date,value}]`; **`Volume Intensity`** → `mention_volume`.
- Empty `{}` / missing timeline → **empty file** (valid no-data).

## All providers

Raw rows carry a **`raw`** field for lineage; bronze stores **`raw_json`** string after normalizers drop inline `raw`.
