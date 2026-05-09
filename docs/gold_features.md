# Gold features

Built by `features/build_gold.py`:

- **`training_examples`:** full join per `(event_time, market_id, asset, narrative)` — labels, scores, `is_candidate_event`.
- **`live_signals`:** subset with `is_candidate_event == true`.

Paths: `gold/.../date=YYYY-MM-DD/data.parquet` (CLI); Dagster may add `partition=…`.

## No-lookahead

- Features: timestamps **`<= event_time`** (belief/narrative lags and trailing stats use prior rows only; past returns at t−1h/4h).
- Labels: **`> event_time`** (forward returns 1h/4h/24h).

Tests: `tests/test_forward_labels_no_lookahead.py`, `tests/test_narrative_features.py`.

## Market tags (`market_tags.csv`)

Required for inclusion: `asset` (BTC/ETH/SOL), `direction` (−1 / +1 for how **higher probability** maps to asset view), `relevance`, `confidence`. Untagged markets are dropped.

## Column blocks (detail)

Formulas match **`docs/alpha_hypothesis.md`**; implementation uses **as-of joins** where noted in code (not plain `.shift()` for irregular live belief snapshots).

| Block | Source module | Notes |
|-------|---------------|--------|
| Belief shock, z-scores | `features/belief.py` | `price` = probability; `belief_shock_abs_1h` = \|directional shock\| |
| Narrative | `features/narrative.py` | Ratios and z from `mention_volume`; tiny history → huge z possible (artifact) |
| Prices / labels | `features/prices.py` | Past and forward returns; directional = `direction * ret` |
| Scoring | `features/scoring.py` | `underreaction_score` — spread + liquidity penalties (see alpha doc) |

**Typical scales:** belief shocks often small (probabilities); narrative acceleration is a ratio; narrative z roughly normal with enough history. **Sample rows:** ignore scale.

## `quality_flags`

Pipe-separated: `missing_future_labels`, `missing_narrative`, `missing_price_lag` when all relevant fields null; empty string if none.

## Candidate flag

`belief_shock_abs_1h >= 0.08`, `underreaction_score > 0`, `confidence >= 0.6`, `relevance ∈ {medium, high}` — MVP heuristic only.
