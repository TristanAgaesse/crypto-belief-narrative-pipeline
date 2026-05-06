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
