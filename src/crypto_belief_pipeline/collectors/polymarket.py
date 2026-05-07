from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from crypto_belief_pipeline.collectors.http import get_json

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _parse_jsonish(value: Any) -> Any:
    """Return a Python object whether ``value`` is already structured or a JSON string.

    Polymarket sometimes returns fields like ``outcomes`` and ``outcomePrices`` as
    JSON-encoded strings rather than native lists. This helper normalizes both shapes.
    Returns ``None`` if parsing fails or input is None.
    """

    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except (TypeError, ValueError):
            return None
    return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return bool(value)


def _format_ts(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.strftime(_TS_FORMAT)


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
        return datetime.fromisoformat(text).astimezone(UTC)
    except ValueError:
        return None


def _tag_labels(market: dict) -> list[str]:
    tags = market.get("tags") or []
    if isinstance(tags, str):
        tags = _parse_jsonish(tags) or []
    labels: list[str] = []
    if isinstance(tags, list):
        for tag in tags:
            if isinstance(tag, dict):
                label = tag.get("label") or tag.get("name") or tag.get("slug")
                if label:
                    labels.append(str(label))
            elif isinstance(tag, str):
                labels.append(tag)
    return labels


def fetch_gamma_markets(
    limit: int = 100,
    offset: int = 0,
    order: str | None = None,
    ascending: bool | None = None,
    active: bool = True,
    closed: bool = False,
) -> list[dict]:
    """Fetch markets from Polymarket's Gamma API.

    Returns the raw list of market objects as returned by the API.
    """

    params: dict[str, Any] = {
        "limit": limit,
        "offset": offset,
        "active": str(active).lower(),
        "closed": str(closed).lower(),
    }
    if order:
        params["order"] = order
    if ascending is not None:
        params["ascending"] = str(bool(ascending)).lower()
    payload = get_json(GAMMA_MARKETS_URL, params=params)
    if isinstance(payload, list):
        return [m for m in payload if isinstance(m, dict)]
    if isinstance(payload, dict):
        data = payload.get("data") or payload.get("markets") or []
        return [m for m in data if isinstance(m, dict)]
    return []


def filter_markets_by_keywords(markets: list[dict], keywords: list[str]) -> list[dict]:
    """Keep markets whose question/slug/category/tags/description mention any keyword.

    Matching is case-insensitive substring matching.
    """

    if not keywords:
        return list(markets)

    needles = [k.strip().lower() for k in keywords if k and k.strip()]
    if not needles:
        return list(markets)

    out: list[dict] = []
    for m in markets:
        haystack_parts: list[str] = []
        for field in ("question", "slug", "category", "description"):
            v = m.get(field)
            if isinstance(v, str):
                haystack_parts.append(v)
        haystack_parts.extend(_tag_labels(m))
        haystack = " ".join(haystack_parts).lower()
        if any(needle in haystack for needle in needles):
            out.append(m)
    return out


def to_raw_market_record(market: dict) -> dict:
    """Convert a Polymarket Gamma market dict to the Step 2 raw markets contract."""

    market_id = (
        market.get("id")
        or market.get("conditionId")
        or market.get("condition_id")
        or market.get("slug")
    )

    category = market.get("category")
    if not category:
        labels = _tag_labels(market)
        category = labels[0] if labels else None

    end_date = market.get("endDate") or market.get("end_date")

    liquidity = (
        _coerce_float(market.get("liquidity")) or _coerce_float(market.get("liquidityNum")) or 0.0
    )
    volume = _coerce_float(market.get("volume")) or _coerce_float(market.get("volumeNum")) or 0.0

    return {
        "market_id": str(market_id) if market_id is not None else None,
        "question": market.get("question"),
        "slug": market.get("slug"),
        "category": category,
        "active": _coerce_bool(market.get("active")),
        "closed": _coerce_bool(market.get("closed")),
        "end_date": end_date,
        "liquidity": float(liquidity),
        "volume": float(volume),
        "raw": market,
    }


_OutcomeRow = tuple[str, float | None, float | None, float | None]


def _merge_market_best_bid_ask(market: dict, rows: list[_OutcomeRow]) -> list[_OutcomeRow]:
    """Attach top-level bestBid/bestAsk to the Yes leg when token bid/ask are absent."""

    mb = _coerce_float(market.get("bestBid") or market.get("best_bid"))
    ma = _coerce_float(market.get("bestAsk") or market.get("best_ask"))
    if mb is None and ma is None:
        return rows
    out: list[_OutcomeRow] = []
    for outcome, price, bid, ask in rows:
        if str(outcome).strip().lower() == "yes":
            if bid is None:
                bid = mb
            if ask is None:
                ask = ma
        out.append((outcome, price, bid, ask))
    return out


def _extract_outcome_prices(market: dict) -> list[_OutcomeRow]:
    """Return list of (outcome, price, best_bid, best_ask) for a market.

    Bid/ask are None when not available; price is None if it can't be parsed.
    Tries ``tokens`` first (richest), then ``outcomes`` + ``outcomePrices``.
    """

    rows: list[_OutcomeRow] = []

    tokens = _parse_jsonish(market.get("tokens"))
    if isinstance(tokens, list) and tokens:
        for tok in tokens:
            if not isinstance(tok, dict):
                continue
            outcome = tok.get("outcome") or tok.get("name") or "Yes"
            price = _coerce_float(tok.get("price"))
            best_bid = _coerce_float(tok.get("bestBid") or tok.get("best_bid"))
            best_ask = _coerce_float(tok.get("bestAsk") or tok.get("best_ask"))
            if price is None and best_bid is None and best_ask is None:
                continue
            rows.append((str(outcome), price, best_bid, best_ask))
        if rows:
            return rows

    outcomes = _parse_jsonish(market.get("outcomes"))
    prices = _parse_jsonish(market.get("outcomePrices"))
    if isinstance(prices, list) and prices:
        if not isinstance(outcomes, list) or not outcomes:
            outcomes = ["Yes" if i == 0 else f"Outcome{i}" for i in range(len(prices))]
        for outcome, price in zip(outcomes, prices, strict=False):
            p = _coerce_float(price)
            if p is None:
                continue
            rows.append((str(outcome), p, None, None))
        if rows:
            return _merge_market_best_bid_ask(market, rows)

    last_price = _coerce_float(market.get("lastTradePrice") or market.get("price"))
    if last_price is not None:
        return _merge_market_best_bid_ask(market, [("Yes", last_price, None, None)])

    return rows


def extract_price_snapshots(
    markets: list[dict],
    snapshot_time: datetime | None = None,
) -> list[dict]:
    """Produce one price snapshot record per market/outcome.

    Skips markets where no price can be extracted.
    """

    ts = _format_ts(snapshot_time or datetime.now(UTC).replace(microsecond=0))

    out: list[dict] = []
    for market in markets:
        rows = _extract_outcome_prices(market)
        if not rows:
            continue
        market_id = (
            market.get("id")
            or market.get("conditionId")
            or market.get("condition_id")
            or market.get("slug")
        )
        if market_id is None:
            continue
        liquidity = (
            _coerce_float(market.get("liquidity"))
            or _coerce_float(market.get("liquidityNum"))
            or 0.0
        )
        volume = (
            _coerce_float(market.get("volume")) or _coerce_float(market.get("volumeNum")) or 0.0
        )
        for outcome, price, best_bid, best_ask in rows:
            out.append(
                {
                    "timestamp": ts,
                    "market_id": str(market_id),
                    "outcome": outcome,
                    "price": price,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "liquidity": float(liquidity),
                    "volume": float(volume),
                    "raw": market,
                }
            )
    return out


def collect_polymarket_raw(
    limit: int = 100,
    keywords: list[str] | None = None,
    *,
    start_time: datetime,
    end_time: datetime,
    snapshot_time: datetime | None = None,
    active: bool = True,
    closed: bool = False,
) -> tuple[list[dict], list[dict], dict[str, Any]]:
    """Fetch -> filter -> map to (markets_records, prices_records, metadata).

    Incremental behavior: include only markets whose `updatedAt` is within `[start_time, end_time)`
    when `updatedAt` is present.
    """

    # Paged discovery (deterministic ordering when supported).
    page_limit = int(limit)
    max_pages = 10
    raw_markets: list[dict] = []
    offset = 0
    st = start_time.astimezone(UTC)
    et = end_time.astimezone(UTC)
    for _ in range(max_pages):
        page = fetch_gamma_markets(
            limit=page_limit,
            offset=offset,
            order="updatedAt",
            ascending=False,
            active=active,
            closed=closed,
        )
        if not page:
            break

        # If the API honors ordering, we can stop once pages are older than the window start.
        parsed_page_times = [
            _parse_ts(m.get("updatedAt") or m.get("updated_at"))
            for m in page
            if isinstance(m, dict)
        ]
        oldest = min((t for t in parsed_page_times if t is not None), default=None)
        raw_markets.extend(page)
        if oldest is not None and oldest < st:
            break
        if len(page) < page_limit:
            break
        offset += page_limit

    filtered = filter_markets_by_keywords(raw_markets, keywords or [])

    windowed: list[dict] = []
    missing_updated_at = 0
    for m in filtered:
        updated_at_raw = m.get("updatedAt") or m.get("updated_at")
        updated_at = _parse_ts(updated_at_raw)
        if updated_at is None:
            missing_updated_at += 1
            continue
        if st <= updated_at < et:
            windowed.append(m)

    market_records = [to_raw_market_record(m) for m in windowed]
    price_records = extract_price_snapshots(windowed, snapshot_time=snapshot_time)

    updated_ats = [
        _format_ts(ts)
        for ts in (_parse_ts(m.get("updatedAt") or m.get("updated_at")) for m in windowed)
        if ts is not None
    ]

    meta: dict[str, Any] = {
        "source": "polymarket",
        "start_time": _format_ts(st),
        "end_time": _format_ts(et),
        "load_timestamp": _format_ts(datetime.now(UTC).replace(microsecond=0)),
        "fetched_markets": len(raw_markets),
        "keyword_filtered_markets": len(filtered),
        "window_qualified_markets": len(windowed),
        "missing_updatedAt_count": missing_updated_at,
        "records_markets": len(market_records),
        "records_prices": len(price_records),
        "min_event_time": min(updated_ats) if updated_ats else None,
        "max_event_time": max(updated_ats) if updated_ats else None,
    }
    return market_records, price_records, meta
