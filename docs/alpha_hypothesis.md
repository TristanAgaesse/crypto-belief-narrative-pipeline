# Alpha hypothesis (draft)

**Story:** belief reprices on information; narratives can amplify attention; liquid crypto may **underreact** short-term. Joint belief + narrative timing may segment BTC/ETH/SOL short-horizon regimes for careful backtesting.

**Not:** profitability claim; production execution stack.

## Implemented formulas (`src/crypto_belief_pipeline/features/`)

Belief (per market, `outcome == "Yes"` when present), event time `t`:

```
price_lag_1h = price.shift(1)  # group (market_id, asset, narrative)
price_lag_4h = price.shift(4)
belief_shock_1h = direction * (price - price_lag_1h)
belief_shock_4h = direction * (price - price_lag_4h)
belief_shock_z_1h = belief_shock_1h / max(std(belief_shock_1h, group), 0.01)
```

`direction` ∈ {-1, 1} from `market_tags.csv`. **Live:** use as-of joins for irregular snapshots (code uses `join_asof` in belief features).

Narrative (per narrative, time-sorted):

```
narrative_volume = mention_volume
narrative_volume_lag_1h = mention_volume.shift(1)
narrative_volume_trailing_mean_24h = mention_volume.shift(1).rolling_mean(24)
narrative_volume_trailing_std_24h = mention_volume.shift(1).rolling_std(24)
narrative_acceleration_1h = narrative_volume / max(narrative_volume_lag_1h, 1e-9)
narrative_z_1h = (narrative_volume - trailing_mean) / max(trailing_std, 1e-9)
```

Prices (Binance 1m, hourly-aligned events):

```
asset_ret_past_1h = close/close_at(t-1h) - 1
future_ret_{1h,4h,24h} = close_at(t+h)/close - 1
directional_price_reaction_1h = direction * asset_ret_past_1h
directional_future_ret_* = direction * future_ret_*
```

Underreaction:

```
underreaction_score =
  2.0 * belief_shock_abs_1h
+ 1.0 * max(narrative_z_1h, 0)
- 1.5 * max(directional_price_reaction_1h, 0)
- 1.0 * spread_penalty    # min(spread/0.10,1) or 0 if null
- 1.0 * illiquidity_penalty   # 1 / 0.5 / 0 by liquidity thresholds
```

**Candidate:** `belief_shock_abs_1h >= 0.08`, `underreaction_score > 0`, `confidence >= 0.6`, `relevance ∈ {medium, high}`.

**Invariant:** features use timestamps `<= event_time`; labels use `> event_time`.

**Kalshi:** optional second venue; ingest scoped via `config/kalshi_keywords.yaml` (BTC/ETH/SOL-aligned subset).
