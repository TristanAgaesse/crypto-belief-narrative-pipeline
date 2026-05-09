# Dagster jobs

## Naming

`<layer_scope>__<domain_or_source>__<cadence>_job` and `_schedule` (same stem).

## Partitions (UTC, closed-open `[start,end)`)

| Tier | Key | Assets (summary) |
|------|-----|------------------|
| Staging (minute) | `YYYY-MM-DDTHH:MM` | `raw_*_staging` (Polymarket, Binance, GDELT, Kalshi, Fear & Greed) |
| Canonical (hourly) | `YYYY-MM-DD-HH:00` | `raw_*`, `bronze_*`, core silver, `silver_kalshi_*`, `silver_fear_greed_*`, gold, `soda_data_quality`, `processing_gaps`, `data_issues`, `markdown_reports` |

Hourly raw compacts staging for that hour; partitions are **overwritable** for replay. If staging is incomplete, Binance/GDELT/Polymarket/Kalshi hourly raw can **backfill from API** for the window.

**Caveats:** Polymarket is current-state (limited historical replay). Binance has windowed history. GDELT may overlap across minute runs—downstream dedupes. **GDELT empty silver is OK for gold** (null narrative); CLI and Dagster both treat missing narrative Parquet under the date prefix like empty partition. Kalshi: scoped by `config/kalshi_keywords.yaml` (see `docs/alpha_hypothesis.md`); REST MVP, no WS.

**State:** partition windows + `state/processing_watermarks/` (no legacy ingest cursors).

## Design

Layer order: raw → bronze → silver → gold → quality → reports. One job per source/cadence where practical.

## Job map

| Job | Schedule | Purpose |
|-----|----------|---------|
| `raw_staging__binance__1m_job` | `*/1` | Binance microbatches |
| `raw_staging__polymarket__5m_job` | `*/5` | Polymarket staging |
| `raw_staging__polymarket_discovery__6h_job` | `0 */6` | Polymarket discovery pulse |
| `raw_staging__gdelt__1h_job` | `0 *` | GDELT staging |
| `raw_staging__kalshi__5m_job` | `*/5` | Kalshi staging |
| `raw_staging__fear_greed__1h_job` | `0 *` | Fear & Greed staging |
| `raw_to_silver__binance__1m_job` | `*/1` | Canonical Binance chain |
| `raw_to_silver__polymarket__5m_job` | `*/5` | Canonical Polymarket |
| `raw_to_silver__polymarket_discovery__6h_job` | `0 */6` | Polymarket @ discovery cadence |
| `raw_to_silver__gdelt__1h_job` | `0 *` | Canonical GDELT |
| `raw_to_silver__kalshi__5m_job` | `*/5` | Canonical Kalshi + silver |
| `raw_to_silver__fear_greed__1h_job` | `0 *` | Canonical Fear & Greed |
| `silver_to_gold__signals__5m_job` | `*/5` | Gold tables |
| `gold__label_maturation__1h_job` | `0 *` | Label refresh |
| `gold_to_quality__hourly_job` | `0 *` | Soda + gaps + issues |
| `quality_to_reports__daily_job` | `0 10 * *` | Markdown reports |
| `full_stack__hourly__manual_job` | manual | Full canonical hour |
| `full_stack__minute__manual_job` | manual | Staging only |

Binance staging is schedule-only (no sensor).

## Runbooks (symptom → check → action)

- **Binance raw HTTP errors:** API/env/bucket; rerun partition; use staging job if only append failed.
- **Sparse Polymarket silver:** `config/markets_keywords.yaml`, response shape; inspect staging/canonical metadata.
- **Polymarket discovery stale:** keywords/`limit`; manual run; compare raw keys vs last 6h.
- **Kalshi sparse books/candles:** `config/kalshi_keywords.yaml`, rate limits; check staging `kalshi_meta`.
- **Empty GDELT silver:** often expected; confirm `data_issues` severity; rerun one partition.
- **Stale “freshness”:** staging runs continuous? canonical windows fresh? raw metadata `input_microbatches_seen` vs `processed`, `window_*_utc`.
- **Gold empty:** upstream silver + partition alignment; thresholds/joins.
- **Labels stale:** maturation horizons; rerun `gold__label_maturation__1h_job`.
- **Quality red:** Soda + `data_issues`; fix checks, rerun `gold_to_quality__hourly_job` (not necessarily ingest).
- **Reports stale:** paths/permissions; rerun after quality green.
- **Full stack failed:** narrow to first failing asset; prefer per-source jobs over full manual.

**Backfill one hour:** Ensure staging for that hour (or rely on API fallback). Materialize raw→…→reports for `YYYY-MM-DD-HH:00`. Confirm `window_*` and microbatch counts on canonical raw.
