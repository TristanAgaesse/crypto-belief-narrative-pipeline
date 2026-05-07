# Limitations

- **Research-only**: no claim of profitability; results can be regime-dependent and unstable.
- **Data quality**: APIs may have gaps, revisions, and survivorship/selection biases.
- **Causality**: belief/narrative/price links can be confounded (news, flows, liquidation cascades).
- **Microstructure**: short-horizon signals can be sensitive to execution, fees, and slippage.
- **Overfitting risk**: multiple-hypothesis testing is likely; requires strong evaluation hygiene.

## Step 3 live collector caveats

- **Polymarket field shapes vary**: Gamma sometimes returns `outcomes` and `outcomePrices` as native lists and sometimes as JSON-encoded strings. The collector parses both, but if the API changes again you may see sparse `live_prices.jsonl` until the parser is updated. Bid/ask are not always available; we never fabricate them.
- **Binance regional restrictions**: the public Binance futures endpoint may return `451`/`403` from some jurisdictions. If that happens the collector raises and the live pipeline fails for that source; sample data via `make run-sample` remains the deterministic evaluation path. A future fallback (CoinGecko or Coinbase) is documented but not implemented.
- **GDELT TimelineVol semantics**: this is news-coverage intensity, not social-media volume. Sparse narratives over short windows can return zero rows; that is data, not an error.
- **Polling, not streaming**: live collection is one-shot polling per `make run-live`. Higher-frequency collection requires a scheduler (cron, GH Actions, etc.) and is out of scope for Step 3.
- **Gamma pagination**: Step 3 fetches a single page (`limit` markets) for simplicity. If you need broader coverage, paginate in a later step.

## Step 4 feature/labeling caveats

- **Simple shift-based belief lags assume regular snapshots**: `features/belief.py` uses `pl.col("price").shift(1)` and `.shift(4)` over each `(market_id, asset, narrative)` group. This is correct only when belief snapshots are at uniform hourly cadence. Live Polymarket polling can be irregular; production should replace shifts with as-of time-window lags keyed off `event_time - 1h` / `event_time - 4h`.
- **Exact timestamp price joins can miss labels**: `features/prices.py` uses exact-equality self-joins to compute `t-1h`, `t-4h`, `t+1h`, `t+4h`, `t+24h` close prices. If event times are not candle-aligned (e.g., a belief snapshot at 11:07 versus 1m candles), labels will be null. A later step can swap this for as-of joins with a tolerance.
- **Manual market tags are required**: only markets present in `data/sample/market_tags.csv` (or the configured path) flow into gold. Unknown markets are dropped intentionally to avoid noisy automatic interpretation, but this means the universe of gold events grows only as the tag file is curated.
- **Small sample data cannot validate profitability**: the sample contains a handful of timestamps; rolling windows and z-scores are degenerate at this scale. Use sample data to validate plumbing and contracts, not to draw any inference about returns.
- **Candidate filter is a heuristic**: `is_candidate_event` thresholds (belief_shock_abs_1h >= 0.08, confidence >= 0.6, etc.) are MVP defaults, not optimized values. Treat them as a starting point for later evaluation.
