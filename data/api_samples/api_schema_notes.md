# API schema notes

Reference captures live under `data/api_samples/` for Step 3. This doc summarizes **vendor shapes**, **Step 2 raw JSONL contracts**, and **parsing gotchas** observed when running collectors against live APIs (2026-05-06).

## Polymarket Gamma (`GET /markets`)

**Top-level response:** list of market objects (`?limit=&active=&closed=`).

| API / raw field | Notes |
|-----------------|--------|
| **`id`** | Primary market id (string, e.g. `540816`); **`conditionId`** is bytes32 hex — collector accepts either |
| **`question`**, **`slug`** | Straight-through to bronze |
| **`category`**, **`tags`** | Often **absent** on live markets; collector sets `category` from `category` or **first tag label**, else **`null`** for all rows is normal |
| **`active`**, **`closed`** | Booleans; strings coerced where needed |
| **`endDate`** | ISO Z → raw `end_date`; optional in bronze (`strict=False` parse) |
| **`liquidity`**, **`volume`** | String decimals and/or `liquidityNum` / `volumeNum` — collector coerces to float |
| **`outcomes`**, **`outcomePrices`** | Often **JSON strings** encoding arrays; collector normalizes via `json.loads` |
| **`tokens`** | When present, per-token **`price`**, **`bestBid`**, **`bestAsk`** drive price rows; when absent, **`outcomePrices`** + market-level **`bestBid`** / **`bestAsk`** / **`lastTradePrice`** are used |
| **`raw`** | Full Gamma dict per JSONL row for lineage (`raw_json` in bronze) |

**Price snapshots (`live_prices.jsonl`):** one row per market **per outcome** (binary markets → **Yes** + **No**). **`best_bid` / `best_ask`** are often populated only on the **Yes** leg; the **No** leg may have **`price`** only — **~50% null bid/ask** is expected. **`spread`** in bronze is `best_ask - best_bid` when both are set.

## Binance USD-M futures klines (`GET fapi/v1/klines`)

**Shape:** array of arrays (length **12** per candle in live samples).

| Index | Field |
|------|--------|
| 0 | `open_time` (ms) |
| 1–4 | `open`, `high`, `low`, `close` (often strings in API) |
| 5 | `volume` |
| 6 | `close_time` (ms) |
| 7 | `quote_volume` |
| 8 | `number_of_trades` |
| 9–11 | taker volumes / ignore |

**Step 2 raw record:** `open_time` / `close_time` as **ISO Z** strings; numeric fields as floats; **`raw`** = original kline array. Error payloads are **JSON objects** (`code`, `msg`), not arrays — collector treats as no klines.

**Silver:** `BTCUSDT` → `asset` **BTC** (mapped symbols only; others default `null`).

## GDELT Doc API v2 (`/api/v2/doc/doc`)

**Modes used:** `timelinevol` (volume intensity as share of monitored coverage). Optional: `timelinetone` for tone series (not used in default collector path).

**HTTP behavior (important):**

- The public API is **heavily rate-limited** (often cited as **~1 request / 5s** per client). **Bursting** (e.g. one request per narrative in a tight loop) produces **429** responses.
- The pipeline uses **`httpx`** with **`query` passed as a proper query parameter** (URL-encoded), a **minimum interval between requests**, **429 / 5xx retries** with **`Retry-After`** when present, and backoff on transport errors — see `collectors/gdelt.py`.
- Responses may include **framed JSON with junk bytes**; parsing reuses **`gdeltdoc.helpers.load_json`** tolerance.

**Typical `timelinevol` frame** (after JSON parse): object with **`timeline`** array of series; each series has **`series`** (column name, e.g. **`Volume Intensity`**) and **`data`**: `[{ "date", "value" }, ...]`. First series’ **`date`** list becomes **`datetime`** in the working DataFrame.

**Step 2 raw row mapping:**

| Normalized field | Source |
|------------------|--------|
| `timestamp` | `datetime` / `timestamp` / `date` / … (see `_TIMESTAMP_COLS`) |
| `mention_volume` | **`Volume Intensity`** (and aliases in `_VOLUME_COLS`) |
| `avg_tone` | Tone columns if present; often **`null`** for `timelinevol`-only pulls |
| `narrative`, `query` | From `config/narratives.yaml` |
| `source` | `"gdelt"` |
| `raw` | Original row dict |

**Empty results:** `{}`, missing `timeline`, or zero-length `timeline` → **empty DataFrame** → **empty JSONL** (file may be a single newline). That is **no data**, not a parse failure.

**Library note:** `gdeltdoc.Filters` builds **`query_string`** (keyword + `&startdatetime=` …). YAML queries are often **`"phrase one" OR "phrase two"`**; passing that as a **single string** makes `Filters` emit **`""phrase one" OR "phrase two""`**, which GDELT rejects. The collector splits on **` OR `** and passes a **keyword list** so the query becomes **`("phrase one" OR "phrase two")`**. The **HTTP call and JSON→DataFrame** step uses **`httpx`** in-repo for spacing, retries on **429**, and correct **`query`** URL encoding (the whole filter string must stay one encoded parameter — `&` inside it is **`%26`**).

## Cross-provider JSONL

All raw rows include a **`raw`** object (or list, for Binance) for lineage. Normalizers strip **`raw`** from the Polars frame and store **`raw_json`** as a string column in bronze.
