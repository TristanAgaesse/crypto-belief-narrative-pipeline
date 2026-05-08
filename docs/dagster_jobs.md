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

## Design principles

- Layer-first orchestration: `raw -> bronze -> silver -> gold -> quality -> reports`.
- Source isolation in ingestion jobs for easier reruns and blast-radius control.
- Single responsibility per scheduled job where possible.
- Debuggability in Dagster UI: job names tell both layer and source at a glance.
- Partition consistency: all schedules materialize the same daily partition key in UTC.

## Job map

| Job | Layer scope | Main assets | Schedule | Purpose |
|---|---|---|---|---|
| `raw_to_silver__binance__1m_job` | raw->bronze->silver | `raw_binance`, `bronze_binance`, `silver_crypto_candles_1m` | `*/1 * * * *` | Fast Binance ingestion and normalization chain. |
| `raw_to_silver__polymarket__5m_job` | raw->bronze->silver | `raw_polymarket`, `bronze_polymarket`, `silver_belief_price_snapshots` | `*/5 * * * *` | Primary Polymarket chain for frequent repricing updates. |
| `raw_to_silver__polymarket_discovery__6h_job` | raw->bronze->silver | `raw_polymarket`, `bronze_polymarket`, `silver_belief_price_snapshots` | `0 */6 * * *` | Lower-frequency Polymarket discovery cadence. |
| `raw_to_silver__gdelt__1h_job` | raw->bronze->silver | `raw_gdelt`, `bronze_gdelt`, `silver_narrative_counts` | `0 * * * *` | Hourly narrative ingestion and normalization. |
| `raw__market_fast__5m_job` | raw | `raw_polymarket`, `raw_binance` | `*/5 * * * *` | Raw-only market heartbeat for rapid ingestion visibility. |
| `silver_to_gold__signals__5m_job` | silver->gold | `gold_training_examples`, `gold_live_signals` | `*/5 * * * *` | Build gold outputs from current silver state. |
| `gold__label_maturation__1h_job` | gold | `gold_training_examples` (+ required neighbor `gold_live_signals`) | `0 * * * *` | Refresh label horizons as forward windows mature. |
| `gold_to_quality__hourly_job` | gold->quality | `soda_data_quality`, `data_issues` | `0 * * * *` | Run DQ checks and domain issue detection. |
| `quality_to_reports__daily_job` | quality->reports | `markdown_reports` | `0 10 * * *` | Publish daily report index linking checks/issues outputs. |
| `full_stack__live__hourly_job` | raw->reports | all live assets | `0 * * * *` | Full live refresh across all layers in one run. |
| `full_stack__sample__manual_job` | raw->reports | sample raw + downstream assets | manual | Manual sample-oriented full stack materialization. |
| `full_refresh__all_layers__manual_job` | raw->reports | all live assets | manual | Manual all-layer refresh/backfill entrypoint. |

## Runbooks (high-signal)

### `raw_to_silver__binance__1m_job`
- Symptom: `raw_binance` fails with HTTP/network errors.
- Check: Binance API health and env (`AWS_ENDPOINT_URL`, credentials, bucket).
- Action: rerun latest partition; if raw succeeded but silver failed, rerun same job (it will replay lineage in order).

### `raw_to_silver__polymarket__5m_job`
- Symptom: no/low rows in `silver_belief_price_snapshots`.
- Check: `config/markets_keywords.yaml` filters and Polymarket response shape drift.
- Action: run this job for the partition; inspect `raw_polymarket` metadata (`missing_updatedAt_count`, window fields).

### `raw_to_silver__polymarket_discovery__6h_job`
- Symptom: expected market universe did not refresh.
- Check: keyword config, active/closed flags, and `limit` in collector config.
- Action: trigger one manual run, then compare raw market keys and row counts to previous 6h run.

### `raw_to_silver__gdelt__1h_job`
- Symptom: empty `silver_narrative_counts`.
- Check: often expected for sparse windows; confirm whether `data_issues` flagged it as high severity.
- Action: rerun one partition; treat persistent empties as data quality signal, not collector crash.

### `raw__market_fast__5m_job`
- Symptom: dashboards stale but full pipelines look healthy.
- Check: this heartbeat run history first (quickest signal of ingestion health).
- Action: if red, inspect collector logs; if green, issue is likely downstream transforms.

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
- Check: `reports/index.md` write path permissions and upstream quality asset status.
- Action: rerun this job after `gold_to_quality__hourly_job` is green.

### `full_stack__live__hourly_job`
- Symptom: end-to-end hourly run failed mid-graph.
- Check: first failing asset in Dagster event log.
- Action: prefer rerunning the narrower source/layer job instead of repeatedly rerunning full stack.

### `full_stack__sample__manual_job`
- Symptom: manual sample materialization fails in Dagster.
- Check: `sample.sample_enabled` and configured sample bucket isolation.
- Action: for deterministic offline validation, prefer CLI path: `make full-sample`.

### `full_refresh__all_layers__manual_job`
- Symptom: backfill/full refresh needed.
- Check: target partition and source API availability before launch.
- Action: run manually off-peak; monitor raw completion first, then quality/report outputs.
