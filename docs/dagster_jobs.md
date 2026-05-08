# Dagster Jobs and Runbooks

This document defines the operational contract for Dagster jobs in this repo:
- naming convention
- job responsibilities by layer
- schedule ownership
- concise runbooks for common failures

## Naming convention

Jobs follow:

`<layer_scope>__<domain_or_source>__<cadence>_job`

Examples:
- `raw_to_silver__binance__1m_job`
- `silver_to_gold__signals__5m_job`
- `gold_to_quality__hourly_job`

Schedules follow the same stem:

`<layer_scope>__<domain_or_source>__<cadence>_schedule`

## Partition strategy (current)

### Two-tier model

- Tier 1 (staging): minute-partitioned append-only ingestion assets:
  - `raw_polymarket_staging`
  - `raw_binance_staging`
  - `raw_gdelt_staging`
  - `raw_kalshi_staging`
- Tier 2 (canonical): hourly-partitioned recomputable assets across all layers:
  - raw canonical: `raw_polymarket`, `raw_binance`, `raw_gdelt`, `raw_kalshi`
  - bronze: `bronze_polymarket`, `bronze_binance`, `bronze_gdelt`, `bronze_kalshi`
  - silver: `silver_belief_price_snapshots`, `silver_crypto_candles_1m`, `silver_narrative_counts`, plus Kalshi research tables (`silver_kalshi_*`)
  - gold: `gold_training_examples`, `gold_live_signals`
  - quality/reports: `soda_data_quality`, `processing_gaps`, `data_issues`, `markdown_reports`

### Partition keys and windows

- Minute staging partition key: `YYYY-MM-DDTHH:MM` (UTC). Schedules anchor to the **last completed minute** so ingest windows never extend into the future.
- Hourly canonical partition key: `YYYY-MM-DD-HH:00` (UTC).
- Processing semantics are always closed-open: `[start,end)` in UTC.

### Compaction and recomputation contract

- Hourly canonical raw assets compact staging microbatches in the matching hour window into one canonical hourly JSONL output per source.
- Canonical downstream assets overwrite their hourly partition outputs, making reruns and backfills deterministic.
- Existing schedule frequencies stay the same; each scheduled canonical run rematerializes the current hour partition.
- Binance/GDELT/Polymarket/Kalshi hourly raw have API fallbacks: if staging is incomplete for the target hour (including partial missing microbatches), they fetch directly for the canonical hour window and write canonical hourly output.

### Source-specific caveats

- Polymarket: current-state API only (no historical query params); historical replay fidelity is limited by what staging captured.
- Binance: best historical compatibility (`startTime`/`endTime` window support).
- GDELT: day-granularity API query behavior may produce overlap across minute staging runs; canonical hourly layers should treat this as expected and rely on downstream dedupe semantics.
- Kalshi: REST-first public Trade API (`config/kalshi_keywords.yaml` for limits and hypothesis scope per `docs/alpha_hypothesis.md`: scoped markets, per-market trades, no global orderbook fallback). Optional WebSocket ingestion is not enabled in MVP.

## Design principles

- Layer-first orchestration: `raw -> bronze -> silver -> gold -> quality -> reports`.
- Source isolation in ingestion jobs for easier reruns and blast-radius control.
- Single responsibility per scheduled job where possible.
- Debuggability in Dagster UI: job names tell both layer and source at a glance.
- Window contract: all ingestion/transform processing uses `[start,end)` UTC semantics.

## Canonical State Model

- Canonical execution state is now:
  - partition windows (`[start,end)`, UTC) selected from Dagster partition keys
  - processing watermarks under `state/processing_watermarks/...` for consumer progress
- Ingestion cursor state is no longer part of the operational contract.
- Operational checks and replay decisions should use window metadata + processing watermarks.

## Job map

| Job | Layer scope | Main assets | Schedule | Purpose |
|---|---|---|---|---|
| `raw_staging__binance__1m_job` | raw staging | `raw_binance_staging` | `*/1 * * * *` | Append minute Binance microbatches for low-latency ingest. |
| `raw_staging__polymarket__5m_job` | raw staging | `raw_polymarket_staging` | `*/5 * * * *` | Append minute Polymarket microbatches on primary cadence. |
| `raw_staging__polymarket_discovery__6h_job` | raw staging | `raw_polymarket_staging` | `0 */6 * * *` | Lower-frequency Polymarket discovery ingest pulse. |
| `raw_staging__gdelt__1h_job` | raw staging | `raw_gdelt_staging` | `0 * * * *` | Append minute GDELT microbatches from hourly trigger. |
| `raw_staging__kalshi__5m_job` | raw staging | `raw_kalshi_staging` | `*/5 * * * *` | Append Kalshi multi-dataset microbatches (markets/events/series/trades/books/candles). |
| `raw_to_silver__binance__1m_job` | canonical raw->silver | `raw_binance`, `bronze_binance`, `silver_crypto_candles_1m` | `*/1 * * * *` | Recompute current hourly canonical Binance chain. |
| `raw_to_silver__polymarket__5m_job` | canonical raw->silver | `raw_polymarket`, `bronze_polymarket`, `silver_belief_price_snapshots` | `*/5 * * * *` | Recompute current hourly canonical Polymarket chain. |
| `raw_to_silver__polymarket_discovery__6h_job` | canonical raw->silver | `raw_polymarket`, `bronze_polymarket`, `silver_belief_price_snapshots` | `0 */6 * * *` | Discovery cadence for canonical hourly Polymarket chain. |
| `raw_to_silver__gdelt__1h_job` | canonical raw->silver | `raw_gdelt`, `bronze_gdelt`, `silver_narrative_counts` | `0 * * * *` | Recompute current hourly canonical GDELT chain. |
| `raw_to_silver__kalshi__5m_job` | canonical raw->silver | `raw_kalshi`, `bronze_kalshi`, all `silver_kalshi_*` | `*/5 * * * *` | Recompute hourly canonical Kalshi normalization + repricing features. |
| `silver_to_gold__signals__5m_job` | canonical silver->gold | `gold_training_examples`, `gold_live_signals` | `*/5 * * * *` | Recompute current hourly gold outputs. |
| `gold__label_maturation__1h_job` | canonical gold | `gold_training_examples` (+ required neighbor `gold_live_signals`) | `0 * * * *` | Hourly label maturation refresh for canonical partition. |
| `gold_to_quality__hourly_job` | canonical gold->quality | `soda_data_quality`, `processing_gaps`, `data_issues` | `0 * * * *` | Run DQ checks, partition-scoped watermark gaps, and issue detection on hourly canonical output. |
| `quality_to_reports__daily_job` | canonical quality->reports | `markdown_reports` | `0 10 * * *` | Daily trigger rematerializing current hour canonical report slice. |
| `full_stack__hourly__manual_job` | canonical raw->reports | all live canonical assets | manual | Manual full canonical hourly refresh/backfill entrypoint. |
| `full_stack__minute__manual_job` | raw staging | staging assets only | manual | Manual minute staging ingest sweep. |

Binance raw staging is schedule-only (`raw_staging__binance__1m_schedule`); there is no Dagster sensor for Binance ingest.

## Runbooks (high-signal)

### `raw_to_silver__binance__1m_job`
- Symptom: `raw_binance` fails with HTTP/network errors.
- Check: Binance API health and env (`AWS_ENDPOINT_URL`, credentials, bucket).
- Action: rerun latest partition; use `raw_staging__binance__1m_job` when only ingest append failed.

### `raw_to_silver__polymarket__5m_job`
- Symptom: no/low rows in `silver_belief_price_snapshots`.
- Check: `config/markets_keywords.yaml` filters and Polymarket response shape drift.
- Action: run this job for the partition; inspect staging + canonical metadata (`missing_updatedAt_count`, window fields).

### `raw_to_silver__polymarket_discovery__6h_job`
- Symptom: expected market universe did not refresh.
- Check: keyword config, active/closed flags, and `limit` in collector config.
- Action: trigger one manual run, then compare raw market keys and row counts to previous 6h run.

### `raw_to_silver__kalshi__5m_job`
- Symptom: sparse order books or candles vs markets.
- Check: `config/kalshi_keywords.yaml` (`keywords` / `asset_aliases`, `scope_markets_to_hypothesis`, `markets_max_pages`, `trades_max_markets_to_query`, `disabled_endpoints`) and Kalshi API rate limits.
- Action: rerun hourly partition; inspect `kalshi_meta` on staging runs and canonical `data_source` metadata for API backfill.

### `raw_to_silver__gdelt__1h_job`
- Symptom: empty `silver_narrative_counts`.
- Check: often expected for sparse windows; confirm whether `data_issues` flagged it as high severity.
- Action: rerun one partition; treat persistent empties as data quality signal, not collector crash.

### Ingestion freshness (replaces standalone heartbeat job)
- Symptom: dashboards stale.
- Check:
  - latest staging runs (`raw_staging__*`) for ingest continuity
  - latest canonical runs (`raw_to_silver__*`) for compacted hourly freshness
  - raw-step metadata:
  - `window_start_utc`, `window_end_utc`
  - `input_microbatches_seen`, `input_microbatches_processed`
- Action: if seen>processed or windows stale, run bounded replay for affected partitions.

### `silver_to_gold__signals__5m_job`
- Symptom: `gold_live_signals` missing or delayed.
- Check: upstream silver asset freshness and partition alignment.
- Action: rerun this job for the partition; if still empty, inspect gold feature thresholds/joins.

### `gold__label_maturation__1h_job`
- Symptom: forward-return labels stale for recent events.
- Check: run timing versus label horizons (1h/4h/24h maturation window).
- Action: rerun latest partition; this is safe and expected as labels mature.

### `gold_to_quality__hourly_job`
- Symptom: pipeline marked unhealthy after gold success.
- Check: `soda_data_quality` failed checks and `data_issues` JSON/MD outputs.
- Action: resolve failing check root cause; rerun this job only (do not rerun ingestion first).

### `quality_to_reports__daily_job`
- Symptom: reports index missing or stale.
- Check: partitioned report output path permissions and upstream quality asset status.
- Action: rerun this job after `gold_to_quality__hourly_job` is green.

### `full_stack__hourly__manual_job`
- Symptom: end-to-end hourly run failed mid-graph.
- Check: first failing asset in Dagster event log.
- Action: this job is manual-only; prefer rerunning narrower source/layer jobs for operational recovery.

### Backfill runbook (hourly partition)
- Goal: recompute one historical canonical hour deterministically.
- Recommended sequence:
  1. Ensure minute staging data exists for that hour (or backfill staging first).
  2. Materialize canonical hourly chain for that hour (`raw_*` -> bronze -> silver -> gold -> quality -> reports).
  3. Verify metadata on canonical raw/bronze steps:
     - `window_start_utc`, `window_end_utc`
     - `input_microbatches_seen`, `input_microbatches_processed`
- Source-specific fallback notes when staging is missing:
  - `raw_binance`: fetches Binance klines for `[start,end)`.
  - `raw_gdelt`: fetches GDELT timeline window for `[start,end)`.
  - `raw_polymarket`: fetches current-state windowed snapshot for `[start,end)` (historical fidelity depends on API behavior).
- Completeness check: canonical hourly assets evaluate expected microbatch slots from source cadence config; if any expected slot is missing, fallback to API backfill is triggered for that partition.
- Note: canonical hourly assets normally compact staged inputs; the sources above additionally support explicit API fallback for missing-hour staging.
