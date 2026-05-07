# Gold features (Step 4)

This document explains how to interpret the Step 4 gold tables:

- `gold/training_examples/date=YYYY-MM-DD/data.parquet`
- `gold/alpha_events/date=YYYY-MM-DD/data.parquet`

These tables are built by `src/crypto_belief_pipeline/features/build_gold.py`.

## What each gold table is for

- **`training_examples`**: the full joined dataset for modelling and research. One row per `(event_time, market_id, asset, narrative)` after joining belief, narrative, and price features. This table includes labels, scoring, and `is_candidate_event`.
- **`alpha_events`**: a filtered subset of `training_examples` where `is_candidate_event == true`. This is a convenient “event list” for event studies and manual inspection.

## No-lookahead rule (most important)

Gold tables are constructed to keep features and labels temporally separated:

- **Features** may only use data at timestamps **`<= event_time`**
- **Labels** may only use data at timestamps **`> event_time`**

In practice:

- Belief features use current and lagged market probability snapshots.
- Narrative features use lagged and trailing windows computed using prior rows only.
- Price features use past returns at `t-1h`/`t-4h`.
- Labels use forward returns at `t+1h`/`t+4h`/`t+24h`.

Unit tests enforce this (see `tests/test_forward_labels_no_lookahead.py` and `tests/test_narrative_features.py`).

## Market tags (manual interpretation)

Markets are only included if they are manually tagged in `data/sample/market_tags.csv` (or the path passed to `build-gold`).

Columns:

- `asset ∈ {BTC, ETH, SOL}`
- `direction ∈ {-1, 1}`: how to interpret a *probability increase* for the tagged market
  - `+1`: higher probability is hypothesized bullish for the asset
  - `-1`: higher probability is hypothesized bearish for the asset
- `relevance ∈ {low, medium, high}` and `confidence ∈ [0, 1]`: priors used by filtering/scoring

Markets without tags are intentionally excluded to avoid noisy automatic interpretation in the MVP.

## Feature blocks

### 1) Belief shock (Polymarket)

Source: `silver/belief_price_snapshots` → `features/belief.py`

The belief probability is `price` (e.g. 0.43 means 43%).

Lag features (MVP):

- `price_lag_1h = price.shift(1)` within `(market_id, asset, narrative)`
- `price_lag_4h = price.shift(4)`

Directional shocks:

- `belief_shock_1h = direction * (price - price_lag_1h)`
- `belief_shock_4h = direction * (price - price_lag_4h)`
- `belief_shock_abs_1h = abs(belief_shock_1h)` (magnitude-only)

Z-score:

- `belief_shock_z_1h = belief_shock_1h / max(std(belief_shock_1h, group), 0.01)`

Interpretation (typical ranges):

- **`belief_shock_1h`**: roughly in \([-1, 1]\), but usually much smaller (probabilities rarely move by >10–20 points in 1h).
- **`belief_shock_z_1h`**: not a strict z-score in the MVP; it’s scaled by a group std with a floor. Use it for rough “surprise” magnitude, not calibration.

Important caveat:

- In live runs, belief snapshots may be irregular; `.shift()` assumes regular hourly cadence. A later step should use as-of joins for `t-1h`/`t-4h`.

### 2) Narrative acceleration (GDELT TimelineVol)

Source: `silver/narrative_counts` → `features/narrative.py`

Core columns:

- `narrative_volume = mention_volume`
- `narrative_volume_lag_1h = mention_volume.shift(1)`
- `narrative_volume_trailing_mean_24h = mention_volume.shift(1).rolling_mean(24)`
- `narrative_acceleration_1h = narrative_volume / max(narrative_volume_lag_1h, 1e-9)`
- `narrative_z_1h = (narrative_volume - trailing_mean) / max(trailing_std, 1e-9)`

Interpretation (typical ranges):

- **`narrative_acceleration_1h`**: ratio; can be >1 on spikes and <1 on fades.
- **`narrative_z_1h`**: with enough history, behaves like a normal z-score (often \([-5, 5]\)).

Common gotcha (sample / very short histories):

- If there is almost no history, trailing std can be ~0, and the denominator floor `1e-9` can produce extremely large `narrative_z_1h` (e.g. 300,000). This is expected for tiny samples and should not be treated as meaningful calibration.

### 3) Price reaction + forward labels (Binance 1m candles)

Source: `silver/crypto_candles_1m` → `features/prices.py`

Past returns:

- `asset_close = close`
- `asset_close_lag_1h`: close at `event_time - 1h` (exact timestamp join)
- `asset_ret_past_1h = asset_close / asset_close_lag_1h - 1`

Forward returns (labels):

- `future_close_1h`: close at `event_time + 1h` (exact join)
- `future_ret_1h = future_close_1h / asset_close - 1`
- similarly for `4h` and `24h`

Directional forms:

- `directional_price_reaction_1h = direction * asset_ret_past_1h`
- `directional_future_ret_1h = direction * future_ret_1h`

Interpretation:

- Past returns indicate whether the asset already moved in the “expected” direction before the event.
- Forward returns are labels for drift/mean-reversion research.

Common gotcha:

- Exact timestamp joins require event times to be candle-aligned. If not aligned, some lags/labels will be null until as-of joins are added.

## Penalties and scoring

### Spread penalty

Computed from Polymarket best bid/ask spread:

- `spread_penalty = 0` if `spread` is null
- else `min(spread / 0.10, 1.0)`

Interpretation:

- A 10 percentage-point spread (or wider) gets full penalty.

### Illiquidity penalty (MVP)

From Polymarket `liquidity`:

- `1.0` if liquidity is null or `<= 0`
- `0.5` if liquidity `< 1000`
- `0.0` otherwise

### Underreaction score

Computed in `features/scoring.py`:

```
underreaction_score =
  2.0 * belief_shock_abs_1h
+ 1.0 * max(narrative_z_1h, 0)
- 1.5 * max(directional_price_reaction_1h, 0)
- 1.0 * spread_penalty
- 1.0 * illiquidity_penalty
```

How to read it:

- Higher belief shock → stronger candidate event
- Higher narrative z → stronger attention acceleration
- Asset already moved up in the expected direction → weaker “underreaction” setup
- Wide spreads / low liquidity → lower quality

Expected range:

- With enough narrative history, scores are typically on the order of **a few units to tens**.
- On tiny samples, narrative z-score can dominate and make scores extremely large; treat those as **sample artifact**, not scale.

## Candidate flag

`is_candidate_event` is an MVP filter:

- `belief_shock_abs_1h >= 0.08`
- `underreaction_score > 0`
- `confidence >= 0.6`
- `relevance ∈ {medium, high}`

## `quality_flags`

Pipe-joined flags for common missingness:

- `missing_future_labels`: all of `future_ret_{1h,4h,24h}` are null
- `missing_narrative`: `narrative_z_1h` is null
- `missing_price_lag`: `asset_ret_past_1h` is null

Empty string means no flags were set.

