"""Kalshi Trade API v2 REST collector (public market data)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from crypto_belief_pipeline.collectors.http import HttpError, get_json
from crypto_belief_pipeline.config import get_settings

logger = logging.getLogger(__name__)

_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _format_ts(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.strftime(_TS_FORMAT)


def _api_base() -> str:
    return get_settings().kalshi_trade_api_base.rstrip("/")


def _url(path: str) -> str:
    p = path if path.startswith("/") else f"/{path}"
    return f"{_api_base()}{p}"


def _envelope(
    *,
    data: dict[str, Any],
    fetched_at: datetime,
    snapshot_time: datetime,
    ingest_batch_id: str,
) -> dict[str, Any]:
    return {
        "fetched_at": _format_ts(fetched_at),
        "snapshot_time": _format_ts(snapshot_time),
        "ingest_batch_id": ingest_batch_id,
        "data": data,
    }


def _disabled(cfg: dict[str, Any], name: str) -> bool:
    raw = cfg.get("disabled_endpoints") or []
    if not isinstance(raw, list):
        return False
    return name in {str(x).strip().lower() for x in raw if str(x).strip()}


def _chunked(seq: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        return [seq]
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def _paginate_dict_items(
    path: str,
    *,
    params_base: dict[str, Any],
    list_key: str,
    max_pages: int = 500,
) -> tuple[list[dict[str, Any]], str | None]:
    """Follow cursor until empty or max_pages; return items and last error message."""

    out: list[dict[str, Any]] = []
    cursor: str | None = None
    last_err: str | None = None
    for _ in range(max_pages):
        params = dict(params_base)
        if cursor:
            params["cursor"] = cursor
        try:
            payload = get_json(_url(path), params=params)
        except HttpError as e:
            last_err = str(e)
            logger.warning("Kalshi %s page failed: %s", path, e)
            break
        except Exception as e:  # pragma: no cover - transport
            last_err = str(e)
            logger.warning("Kalshi %s page failed: %s", path, e)
            break
        if not isinstance(payload, dict):
            last_err = "non_object_response"
            break
        chunk = payload.get(list_key)
        if not isinstance(chunk, list):
            last_err = f"missing_{list_key}"
            break
        for it in chunk:
            if isinstance(it, dict):
                out.append(it)
        nxt = payload.get("cursor")
        cursor = str(nxt) if nxt else None
        if not cursor:
            break
    return out, last_err


def _market_text_haystack(m: dict[str, Any]) -> str:
    parts: list[str] = []
    for k in ("ticker", "yes_sub_title", "no_sub_title", "event_ticker", "title", "subtitle"):
        v = m.get(k)
        if isinstance(v, str):
            parts.append(v)
    return " ".join(parts).lower()


def market_matches_hypothesis_scope(market: dict[str, Any], cfg: dict[str, Any]) -> bool:
    """True if market text matches ``keywords`` or ``asset_aliases``.

    Scoped to the BTC/ETH/SOL belief layer in the alpha config.
    """

    keywords = cfg.get("keywords") or []
    needles = (
        [str(k).strip().lower() for k in keywords if str(k).strip()]
        if isinstance(keywords, list)
        else []
    )
    aliases = cfg.get("asset_aliases") or {}
    has_alias_patterns = False
    if isinstance(aliases, dict):
        for _asset, pats in aliases.items():
            if isinstance(pats, list) and any(str(p).strip() for p in pats):
                has_alias_patterns = True
                break
    if not needles and not has_alias_patterns:
        return True

    hay = _market_text_haystack(market)
    if needles and any(n in hay for n in needles):
        return True
    if isinstance(aliases, dict):
        for _asset, pats in aliases.items():
            if not isinstance(pats, list):
                continue
            for p in pats:
                sub = str(p).lower()
                if sub and sub in hay:
                    return True
    return False


def fetch_markets(cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    status = str(cfg.get("markets_status") or "open").strip() or "open"
    limit = int(cfg.get("markets_page_limit") or 500)
    limit = max(1, min(limit, 1000))
    max_pages_default = int(cfg.get("markets_max_pages") or 500)
    max_pages_default = max(1, min(max_pages_default, 500))

    base_params: dict[str, Any] = {"limit": limit, "status": status}
    st = cfg.get("markets_series_ticker")
    if isinstance(st, str) and st.strip():
        base_params["series_ticker"] = st.strip()
    et = cfg.get("markets_event_ticker")
    if isinstance(et, str) and et.strip():
        base_params["event_ticker"] = et.strip()

    raw_tickers = cfg.get("market_tickers")
    if isinstance(raw_tickers, list) and raw_tickers:
        cleaned = [str(t).strip() for t in raw_tickers if str(t).strip()]
        seen: set[str] = set()
        uniq: list[str] = []
        for t in cleaned:
            if t not in seen:
                seen.add(t)
                uniq.append(t)
        if not uniq:
            return _paginate_dict_items(
                "/markets",
                params_base=base_params,
                list_key="markets",
                max_pages=max_pages_default,
            )
        chunk_size = int(cfg.get("markets_tickers_chunk_size") or 50)
        chunk_size = max(1, min(chunk_size, 100))
        per_chunk_pages = int(cfg.get("markets_ticker_chunk_max_pages") or 10)
        per_chunk_pages = max(1, min(per_chunk_pages, 50))
        by_ticker: dict[str, dict[str, Any]] = {}
        last_err: str | None = None
        for chunk in _chunked(uniq, chunk_size):
            params = {**base_params, "tickers": ",".join(chunk)}
            rows, err = _paginate_dict_items(
                "/markets",
                params_base=params,
                list_key="markets",
                max_pages=per_chunk_pages,
            )
            if err:
                last_err = err
            for m in rows:
                if isinstance(m, dict) and isinstance(m.get("ticker"), str):
                    by_ticker[m["ticker"]] = m
        return list(by_ticker.values()), last_err

    return _paginate_dict_items(
        "/markets",
        params_base=base_params,
        list_key="markets",
        max_pages=max_pages_default,
    )


def fetch_events(cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    limit = int(cfg.get("events_page_limit") or 500)
    limit = max(1, min(limit, 200))
    params: dict[str, Any] = {"limit": limit}
    max_pages = int(cfg.get("events_max_pages") or 100)
    max_pages = max(1, min(max_pages, 500))
    return _paginate_dict_items(
        "/events", params_base=params, list_key="events", max_pages=max_pages
    )


def fetch_trades(cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    max_rows = int(cfg.get("trades_max_rows") or 8000)
    max_rows = max(0, min(max_rows, 50_000))
    if max_rows == 0:
        return [], None
    # Hard cap pages: a buggy API could return ``cursor`` forever with empty ``trades``,
    # which would spin a ``while len(out) < max_rows`` loop indefinitely.
    max_pages = int(cfg.get("trades_max_pages") or 500)
    max_pages = max(1, min(max_pages, 2000))
    out: list[dict[str, Any]] = []
    cursor: str | None = None
    last_err: str | None = None
    for _ in range(max_pages):
        if len(out) >= max_rows:
            break
        params: dict[str, Any] = {"limit": min(1000, max_rows - len(out))}
        if cursor:
            params["cursor"] = cursor
        tt = cfg.get("trade_ticker")
        if isinstance(tt, str) and tt.strip():
            params["ticker"] = tt.strip()
        if cfg.get("trade_min_ts") is not None:
            params["min_ts"] = int(cfg["trade_min_ts"])
        if cfg.get("trade_max_ts") is not None:
            params["max_ts"] = int(cfg["trade_max_ts"])
        try:
            payload = get_json(_url("/markets/trades"), params=params)
        except HttpError as e:
            last_err = str(e)
            break
        except Exception as e:  # pragma: no cover
            last_err = str(e)
            break
        if not isinstance(payload, dict):
            last_err = "non_object_response"
            break
        chunk = payload.get("trades")
        if not isinstance(chunk, list):
            last_err = "missing_trades"
            break
        for it in chunk:
            if isinstance(it, dict):
                out.append(it)
        nxt = payload.get("cursor")
        cursor = str(nxt) if nxt else None
        if not cursor:
            break
    return out[:max_rows], last_err


def fetch_series_detail(series_ticker: str) -> dict[str, Any] | None:
    try:
        payload = get_json(_url(f"/series/{series_ticker}"))
    except HttpError:
        return None
    except Exception:  # pragma: no cover
        return None
    if not isinstance(payload, dict):
        return None
    s = payload.get("series")
    return s if isinstance(s, dict) else None


def fetch_orderbook(market_ticker: str) -> dict[str, Any] | None:
    try:
        payload = get_json(_url(f"/markets/{market_ticker}/orderbook"))
    except HttpError:
        return None
    except Exception:  # pragma: no cover
        return None
    return payload if isinstance(payload, dict) else None


def fetch_candlesticks(
    *,
    series_ticker: str,
    market_ticker: str,
    period_minutes: int,
    start_ts: int,
    end_ts: int,
) -> list[dict[str, Any]]:
    params = {
        "period_interval": int(period_minutes),
        "start_ts": int(start_ts),
        "end_ts": int(end_ts),
    }
    try:
        payload = get_json(
            _url(f"/series/{series_ticker}/markets/{market_ticker}/candlesticks"),
            params=params,
        )
    except HttpError:
        return []
    except Exception:  # pragma: no cover
        return []
    if not isinstance(payload, dict):
        return []
    candles = payload.get("candlesticks") or []
    return [c for c in candles if isinstance(c, dict)]


def collect_kalshi_raw(
    *,
    start_time: datetime,
    end_time: datetime,
    snapshot_time: datetime,
    ingest_batch_id: str,
    cfg: dict[str, Any],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    """Pull Kalshi datasets for one ingest batch.

    Returns:
        payloads: ``markets``, ``events``, ``series``, ``trades``, ``orderbooks``,
            ``candlesticks``
        meta: counts, errors, timing
    """

    now = datetime.now(UTC).replace(microsecond=0)
    meta: dict[str, Any] = {
        "start_time": _format_ts(start_time),
        "end_time": _format_ts(end_time),
        "snapshot_time": _format_ts(snapshot_time),
        "endpoint_errors": {},
    }

    markets_raw: list[dict[str, Any]] = []
    events_raw: list[dict[str, Any]] = []
    trades_raw: list[dict[str, Any]] = []
    series_raw: list[dict[str, Any]] = []
    orderbooks_raw: list[dict[str, Any]] = []
    candles_raw: list[dict[str, Any]] = []

    if not _disabled(cfg, "markets"):
        markets_raw, m_err = fetch_markets(cfg)
        if m_err:
            meta["endpoint_errors"]["markets"] = m_err

    scope_markets = bool(cfg.get("scope_markets_to_hypothesis", True))
    filtered_markets = (
        [m for m in markets_raw if isinstance(m, dict) and market_matches_hypothesis_scope(m, cfg)]
        if scope_markets
        else list(markets_raw)
    )
    markets_for_downstream = filtered_markets

    if not _disabled(cfg, "events"):
        events_raw, e_err = fetch_events(cfg)
        if e_err:
            meta["endpoint_errors"]["events"] = e_err
    if cfg.get("scope_events_to_markets", True) and markets_for_downstream:
        evt_ids = {
            str(m.get("event_ticker"))
            for m in markets_for_downstream
            if isinstance(m.get("event_ticker"), str) and m.get("event_ticker")
        }
        if evt_ids:
            events_raw = [
                e
                for e in events_raw
                if isinstance(e, dict) and str(e.get("event_ticker") or "") in evt_ids
            ]

    if not _disabled(cfg, "trades"):
        trades_max_total = int(cfg.get("trades_max_rows") or 8000)
        trades_max_total = max(0, min(trades_max_total, 50_000))
        scope_trades = bool(cfg.get("trades_scope_to_filtered_markets", True))
        tickers_for_trades = [
            str(m["ticker"])
            for m in markets_for_downstream
            if isinstance(m, dict) and isinstance(m.get("ticker"), str) and m.get("ticker")
        ]
        max_mk = int(cfg.get("trades_max_markets_to_query") or 40)
        max_mk = max(0, min(max_mk, 200))
        tickers_for_trades = tickers_for_trades[:max_mk]
        if scope_trades and tickers_for_trades and trades_max_total > 0:
            min_ts = int(start_time.timestamp())
            max_ts = int(end_time.timestamp())
            per_cap = max(50, trades_max_total // max(1, len(tickers_for_trades)))
            per_cap = min(per_cap, int(cfg.get("trades_per_market_row_cap") or 2000))
            per_pages = int(cfg.get("trades_per_market_max_pages") or 20)
            per_pages = max(1, min(per_pages, 100))
            trades_raw = []
            last_trade_err: str | None = None
            for tk in tickers_for_trades:
                if len(trades_raw) >= trades_max_total:
                    break
                sub_cfg = {
                    **cfg,
                    "trade_ticker": tk,
                    "trade_min_ts": min_ts,
                    "trade_max_ts": max_ts,
                    "trades_max_rows": min(per_cap, trades_max_total - len(trades_raw)),
                    "trades_max_pages": per_pages,
                }
                chunk, err = fetch_trades(sub_cfg)
                trades_raw.extend(chunk)
                if err:
                    last_trade_err = err
            trades_raw = trades_raw[:trades_max_total]
            if last_trade_err:
                meta["endpoint_errors"]["trades"] = last_trade_err
        elif not scope_trades and trades_max_total > 0:
            trades_raw, t_err = fetch_trades(cfg)
            if t_err:
                meta["endpoint_errors"]["trades"] = t_err
        # Scoped mode with zero hypothesis markets: leave ``trades_raw`` empty (no global firehose).

    series_tickers: set[str] = set()
    for m in markets_for_downstream:
        st = m.get("series_ticker") or m.get("mve_collection_ticker")
        if isinstance(st, str) and st:
            series_tickers.add(st)
    for e in events_raw:
        st = e.get("series_ticker")
        if isinstance(st, str) and st:
            series_tickers.add(st)

    max_series = int(cfg.get("max_series_fetch") or 120)
    max_series = max(0, min(max_series, 500))
    if not _disabled(cfg, "series"):
        for i, st in enumerate(sorted(series_tickers)):
            if i >= max_series:
                break
            detail = fetch_series_detail(st)
            if detail:
                series_raw.append(detail)

    # Orderbooks + candles: prioritize hypothesis-scoped markets by keyword overlap.
    keywords = cfg.get("keywords") or []
    needles = (
        [str(k).strip().lower() for k in keywords if str(k).strip()]
        if isinstance(keywords, list)
        else []
    )

    ranked = list(markets_for_downstream)
    ranked.sort(
        key=lambda m: float(str(m.get("volume_24h_fp") or "0").replace(",", "") or 0.0),
        reverse=True,
    )

    ob_candidates: list[dict[str, Any]] = []
    for m in ranked:
        if len(ob_candidates) >= int(cfg.get("max_markets_orderbook") or 40):
            break
        ticker_raw = m.get("ticker")
        if not isinstance(ticker_raw, str) or not ticker_raw:
            continue
        hay = _market_text_haystack(m)
        if needles and not any(n in hay for n in needles):
            continue
        ob_candidates.append(m)

    if not ob_candidates and ranked and bool(cfg.get("orderbook_fallback_to_volume", False)):
        ob_candidates = ranked[: int(cfg.get("max_markets_orderbook") or 40)]

    if not _disabled(cfg, "orderbooks"):
        for m in ob_candidates:
            tk = str(m.get("ticker") or "")
            if not tk:
                continue
            ob = fetch_orderbook(tk)
            if ob is None:
                continue
            orderbooks_raw.append(
                _envelope(
                    data={"market_ticker": tk, "orderbook": ob},
                    fetched_at=now,
                    snapshot_time=snapshot_time,
                    ingest_batch_id=ingest_batch_id,
                )
            )

    period_min = int(cfg.get("candle_period_minutes") or 15)
    period_min = max(1, min(period_min, 1440))
    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())

    if not _disabled(cfg, "candlesticks"):
        cap = int(cfg.get("max_markets_candles") or 25)
        for m in ob_candidates[:cap]:
            m_tk = m.get("ticker")
            e_tk = m.get("event_ticker")
            s_tk = m.get("series_ticker") or m.get("mve_collection_ticker")
            if not isinstance(m_tk, str) or not isinstance(s_tk, str):
                continue
            candles = fetch_candlesticks(
                series_ticker=s_tk,
                market_ticker=m_tk,
                period_minutes=period_min,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            for c in candles:
                candles_raw.append(
                    _envelope(
                        data={
                            "market_ticker": m_tk,
                            "event_ticker": e_tk if isinstance(e_tk, str) else None,
                            "series_ticker": s_tk,
                            "candle": c,
                            "period_minutes": period_min,
                        },
                        fetched_at=now,
                        snapshot_time=snapshot_time,
                        ingest_batch_id=ingest_batch_id,
                    )
                )

    markets_out = [
        _envelope(
            data=m,
            fetched_at=now,
            snapshot_time=snapshot_time,
            ingest_batch_id=ingest_batch_id,
        )
        for m in markets_for_downstream
    ]
    events_out = [
        _envelope(
            data=e,
            fetched_at=now,
            snapshot_time=snapshot_time,
            ingest_batch_id=ingest_batch_id,
        )
        for e in events_raw
    ]
    series_out = [
        _envelope(
            data=s,
            fetched_at=now,
            snapshot_time=snapshot_time,
            ingest_batch_id=ingest_batch_id,
        )
        for s in series_raw
    ]
    trades_out = [
        _envelope(
            data=t,
            fetched_at=now,
            snapshot_time=snapshot_time,
            ingest_batch_id=ingest_batch_id,
        )
        for t in trades_raw
    ]

    meta.update(
        {
            "markets_rows": len(markets_out),
            "events_rows": len(events_out),
            "series_rows": len(series_out),
            "trades_rows": len(trades_out),
            "orderbooks_rows": len(orderbooks_raw),
            "candlesticks_rows": len(candles_raw),
        }
    )

    return (
        {
            "markets": markets_out,
            "events": events_out,
            "series": series_out,
            "trades": trades_out,
            "orderbooks": orderbooks_raw,
            "candlesticks": candles_raw,
        },
        meta,
    )


__all__ = [
    "collect_kalshi_raw",
    "fetch_candlesticks",
    "fetch_events",
    "fetch_markets",
    "fetch_orderbook",
    "fetch_series_detail",
    "fetch_trades",
    "market_matches_hypothesis_scope",
]
