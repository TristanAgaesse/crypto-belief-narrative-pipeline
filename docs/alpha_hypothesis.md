# Alpha hypothesis (draft)

This repository tests a research hypothesis:

- **Belief repricing**: prediction market probabilities move on new information.
- **Narrative acceleration**: narratives can amplify attention/positioning dynamics.
- **Short-horizon underreaction**: liquid crypto spot/perp markets may underreact over short windows.

If belief repricing and narrative acceleration co-occur (or lead/lag), the joint signal may help segment
regimes for short-horizon BTC/ETH/SOL returns (drift vs reversal), suitable for careful backtesting.

Non-goals:
- This is not a claim of profitability.
- This is not production trading infrastructure.

## MVP formulas (Step 4)

These are the exact formulas implemented in `src/crypto_belief_pipeline/features/`. They are research-grade
defaults and intentionally simple; later steps can refine each component.

### Belief shock

For each Polymarket market (preferring `outcome == "Yes"` when present), at event time `t`:

```
price_lag_1h    = price.shift(1) over (market_id, asset, narrative)
price_lag_4h    = price.shift(4) over (market_id, asset, narrative)
belief_shock_1h = direction * (price - price_lag_1h)
belief_shock_4h = direction * (price - price_lag_4h)
belief_shock_z_1h = belief_shock_1h / max(std(belief_shock_1h, group), 0.01)
```

`direction` is +1 if a rising market probability is hypothesized to be bullish for the asset, -1 if bearish
(curated manually in `data/sample/market_tags.csv`). The MVP assumes regular hourly snapshots; production
should replace `.shift()` with as-of time-window joins for irregular data.

### Narrative acceleration

For each narrative (sorted by time):

```
narrative_volume                   = mention_volume
narrative_volume_lag_1h            = mention_volume.shift(1)
narrative_volume_trailing_mean_24h = mention_volume.shift(1).rolling_mean(24)
narrative_volume_trailing_std_24h  = mention_volume.shift(1).rolling_std(24)
narrative_acceleration_1h          = narrative_volume / max(narrative_volume_lag_1h, 1e-9)
narrative_z_1h                     = (narrative_volume - trailing_mean) / max(trailing_std, 1e-9)
```

The trailing windows use `shift(1)` to enforce that they only see prior rows.

### Price features and forward labels

For each asset (Binance 1m candles with hourly-aligned events):

```
asset_close          = close
asset_close_lag_1h   = close at event_time - 1h
asset_close_lag_4h   = close at event_time - 4h
asset_ret_past_1h    = asset_close / asset_close_lag_1h - 1
asset_ret_past_4h    = asset_close / asset_close_lag_4h - 1
future_close_{1h,4h,24h} = close at event_time + {1h, 4h, 24h}
future_ret_{1h,4h,24h}   = future_close_X / asset_close - 1
```

Directional labels:

```
directional_price_reaction_1h   = direction * asset_ret_past_1h
directional_future_ret_{1h,4h,24h} = direction * future_ret_X
```

### Underreaction score

```
underreaction_score =
    2.0 * belief_shock_abs_1h
  + 1.0 * max(narrative_z_1h, 0)
  - 1.5 * max(directional_price_reaction_1h, 0)
  - 1.0 * spread_penalty
  - 1.0 * illiquidity_penalty
```

Where:

```
spread_penalty       = 0 if spread is null else min(spread / 0.10, 1.0)
illiquidity_penalty  = 1.0 if liquidity is null or <= 0
                       0.5 if liquidity < 1000
                       0.0 otherwise
```

### Candidate event filter

```
is_candidate_event =
    belief_shock_abs_1h >= 0.08
    AND underreaction_score > 0
    AND confidence >= 0.6
    AND relevance in {"medium", "high"}
```

### No-lookahead invariant

Features only use timestamps `<= event_time`. Labels only use timestamps `> event_time`.
