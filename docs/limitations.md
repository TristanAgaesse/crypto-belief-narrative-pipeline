# Limitations

- Research only; no profitability claim; regime risk and overfitting.
- API gaps, bias, confounders; short-horizon signals are execution-sensitive.

## Live ingest

- **Polymarket:** Gamma field shapes vary (lists vs JSON strings for outcomes/prices). Bid/ask often missing; not fabricated.
- **Binance:** `451`/`403` possible by region → live run fails for that source; use sample path to validate plumbing.
- **GDELT:** coverage intensity, not social volume; sparse windows → empty rows; rate limits; treat as optional.
- **Polling:** one-shot per run unless scheduled (Dagster/cron). Gamma uses a single `limit` page unless you extend pagination.

## Kalshi (tier-2)

- **`trade_confirm_score`:** cumulative size as-of each snapshot `as_of` (`executed_at <= as_of`), not ingest time.

## Gold / features

- Belief lags: `join_asof` in `(market_id, asset, narrative)`; irregular snapshots → null lags if no prior row. See `price_lag_*_source_time` / `*_age`.
- Price labels: bounded as-of joins; missing candles → null labels, not stale prices.
- **Market tags:** only rows in `data/sample/market_tags.csv` (or configured path) enter gold.
- **Sample data:** too small for meaningful rolling stats—validate contracts only.
- **`is_candidate_event`:** heuristic thresholds, not tuned.
